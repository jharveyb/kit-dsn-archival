use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

const LISTING_URL: &str = "https://www.dsn.kastel.kit.edu/bitcoin/snapshots/?curl";
/// The listing returns URLs on dsn.tm.kit.edu, which 301-redirect here.
const CANONICAL_HOST: &str = "www.dsn.kastel.kit.edu";
const MAX_ATTEMPTS: u32 = 4;

#[derive(Parser)]
#[command(version, about = "Mirror the KIT DSN Bitcoin network snapshot dataset")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

/// Location of the mirror on disk, shared by all subcommands.
#[derive(Args)]
struct DataDirOpt {
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,
}

impl DataDirOpt {
    fn listing(&self) -> PathBuf {
        self.data_dir.join("listing.txt")
    }

    fn manifest(&self) -> PathBuf {
        self.data_dir.join("manifest.jsonl")
    }

    fn raw(&self) -> PathBuf {
        self.data_dir.join("raw")
    }

    fn compressed(&self) -> PathBuf {
        self.data_dir.join("compressed")
    }
}

/// Snapshot date bounds, shared by subcommands that iterate the dataset.
#[derive(Args)]
struct DateRange {
    /// Only process snapshots dated on or after this day (YYYYMMDD)
    #[arg(long)]
    from: Option<u32>,
    /// Only process snapshots dated on or before this day (YYYYMMDD)
    #[arg(long)]
    to: Option<u32>,
}

impl DateRange {
    fn contains(&self, date: u32) -> bool {
        self.from.is_none_or(|f| date >= f) && self.to.is_none_or(|t| date <= t)
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Fetch the snapshot listing from the server and save it
    List {
        #[command(flatten)]
        dirs: DataDirOpt,
    },
    /// Download snapshot files concurrently into <data-dir>/raw/
    Fetch {
        /// Number of concurrent connections
        #[arg(long, short = 'j', default_value = "4")]
        jobs: NonZeroUsize,
        #[command(flatten)]
        range: DateRange,
        #[command(flatten)]
        dirs: DataDirOpt,
        /// Re-download the listing before fetching
        #[arg(long)]
        refresh: bool,
    },
    /// Compress mirrored snapshots per-file with `zstd --ultra -22` into <data-dir>/compressed/
    Compress {
        /// Number of zstd processes to run in parallel
        #[arg(long, short = 'j', default_value = "1")]
        jobs: NonZeroUsize,
        /// Threads per zstd process (-T); 0 lets zstd use all cores. Note: zstd
        /// only engages extra threads on inputs of hundreds of MB, so for these
        /// files --jobs is what buys parallelism.
        #[arg(long, short = 'T', default_value_t = 0)]
        threads: u32,
        #[command(flatten)]
        range: DateRange,
        #[command(flatten)]
        dirs: DataDirOpt,
    },
    /// Check the local mirror against the listing and manifest
    Verify {
        #[command(flatten)]
        dirs: DataDirOpt,
        /// Recompute sha256 of each local file and compare with the manifest
        #[arg(long)]
        hash: bool,
    },
}

#[derive(Serialize, Deserialize, Clone)]
struct ManifestEntry {
    file: String,
    url: String,
    size: u64,
    sha256: String,
    last_modified: Option<String>,
    etag: Option<String>,
    fetched_at: u64,
}

/// Result of comparing a local file against its manifest entry.
enum FileCheck {
    Missing,
    /// On disk but not recorded in the manifest.
    Unrecorded { size: u64 },
    SizeMismatch { disk: u64, manifest: u64 },
    HashMismatch { size: u64 },
    Ok { size: u64 },
}

/// Compare a local file against its manifest entry; `check_hash` re-hashes the
/// file contents (expensive) instead of trusting the size alone.
fn check_file(path: &Path, entry: Option<&ManifestEntry>, check_hash: bool) -> Result<FileCheck> {
    let Ok(meta) = path.metadata() else {
        return Ok(FileCheck::Missing);
    };
    let size = meta.len();
    let Some(entry) = entry else {
        return Ok(FileCheck::Unrecorded { size });
    };
    if size != entry.size {
        return Ok(FileCheck::SizeMismatch {
            disk: size,
            manifest: entry.size,
        });
    }
    if check_hash {
        let bytes = std::fs::read(path)?;
        if hex::encode(Sha256::digest(&bytes)) != entry.sha256 {
            return Ok(FileCheck::HashMismatch { size });
        }
    }
    Ok(FileCheck::Ok { size })
}

fn rewrite_host(url: &str) -> String {
    url.replace("://dsn.tm.kit.edu/", &format!("://{CANONICAL_HOST}/"))
}

fn filename_of(url: &str) -> Result<&str> {
    url.rsplit('/')
        .next()
        .filter(|f| !f.is_empty())
        .with_context(|| format!("cannot extract filename from {url}"))
}

/// Parse the YYYYMMDD prefix of a snapshot filename.
fn date_of(filename: &str) -> Result<u32> {
    let prefix = filename.get(..8).context("filename shorter than 8 chars")?;
    let date: u32 = prefix
        .parse()
        .with_context(|| format!("non-numeric date prefix in {filename}"))?;
    if !(19000101..=99991231).contains(&date) {
        bail!("implausible date {date} in {filename}");
    }
    Ok(date)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn progress_bar(len: usize) -> Result<ProgressBar> {
    Ok(
        ProgressBar::new(len as u64).with_style(ProgressStyle::with_template(
            "{bar:40} {pos}/{len} [{elapsed_precise}<{eta_precise}] {msg}",
        )?),
    )
}

/// Drive `work` over `items`, `jobs` at a time, under a progress bar.
///
/// `work` moves each item into its future and returns it alongside the result,
/// so `on_result` can do per-item bookkeeping (results arrive in completion
/// order, not input order).
async fn for_each_parallel<I, T, Fut>(
    jobs: NonZeroUsize,
    items: Vec<I>,
    work: impl Fn(I) -> Fut,
    mut on_result: impl FnMut(I, Result<T>, &ProgressBar) -> Result<()>,
) -> Result<()>
where
    Fut: Future<Output = (I, Result<T>)>,
{
    let bar = progress_bar(items.len())?;

    // Futures are running on a single thread, which is fine for file downloads.
    let mut results =
        futures::stream::iter(items.into_iter().map(work)).buffer_unordered(jobs.get());
    while let Some((item, result)) = results.next().await {
        on_result(item, result, &bar)?;
        bar.inc(1);
    }
    bar.finish();
    Ok(())
}

fn build_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(concat!("kit-dsn-archival/", env!("CARGO_PKG_VERSION")))
        .gzip(false) // keep transfers identity-encoded so Content-Length verification is exact
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(600))
        .build()
        .context("building HTTP client")
}

async fn fetch_listing(client: &reqwest::Client) -> Result<Vec<String>> {
    let body = client
        .get(LISTING_URL)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let mut urls: Vec<String> = body
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(rewrite_host)
        .collect();
    // Server lists newest first; store oldest first for stable diffs.
    urls.sort();
    urls.dedup();
    Ok(urls)
}

async fn load_or_fetch_listing(
    client: &reqwest::Client,
    dirs: &DataDirOpt,
    refresh: bool,
) -> Result<Vec<String>> {
    let path = dirs.listing();
    if !refresh && path.exists() {
        let body = tokio::fs::read_to_string(&path).await?;
        return Ok(body.lines().map(str::to_owned).collect());
    }
    let urls = fetch_listing(client).await?;
    tokio::fs::create_dir_all(&dirs.data_dir).await?;
    tokio::fs::write(&path, urls.join("\n") + "\n").await?;
    Ok(urls)
}

fn load_manifest(dirs: &DataDirOpt) -> Result<HashMap<String, ManifestEntry>> {
    let path = dirs.manifest();
    let mut map = HashMap::new();
    if !path.exists() {
        return Ok(map);
    }
    let body = std::fs::read_to_string(&path)?;
    for line in body.lines().filter(|l| !l.trim().is_empty()) {
        let entry: ManifestEntry =
            serde_json::from_str(line).with_context(|| format!("bad manifest line: {line}"))?;
        // Later entries win (re-downloads supersede older records).
        map.insert(entry.file.clone(), entry);
    }
    Ok(map)
}

async fn download_one(client: &reqwest::Client, url: &str, dest: &Path) -> Result<ManifestEntry> {
    let part = dest.with_extension("json.part");
    let mut last_err = None;
    for attempt in 1..=MAX_ATTEMPTS {
        if attempt > 1 {
            tokio::time::sleep(Duration::from_secs(4u64.pow(attempt - 1))).await;
        }
        match try_download(client, url, &part).await {
            Ok(entry) => {
                tokio::fs::rename(&part, dest).await?;
                return Ok(entry);
            }
            Err(e) => last_err = Some(e),
        }
    }
    let _ = tokio::fs::remove_file(&part).await;
    Err(last_err
        .unwrap()
        .context(format!("giving up on {url} after {MAX_ATTEMPTS} attempts")))
}

/// Stream `url` into `part`, verifying against the response body size, and
/// return the manifest record for the download.
async fn try_download(client: &reqwest::Client, url: &str, part: &Path) -> Result<ManifestEntry> {
    let file = filename_of(url)?.to_owned();
    let resp = client.get(url).send().await?.error_for_status()?;
    let header = |name: &str| {
        resp.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let expected = resp.content_length();
    let last_modified = header("last-modified");
    let etag = header("etag");

    let mut out = tokio::fs::File::create(part).await?;
    let mut hasher = Sha256::new();
    let mut written: u64 = 0;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        hasher.update(&chunk);
        out.write_all(&chunk).await?;
        written += chunk.len() as u64;
    }
    out.flush().await?;
    out.sync_all().await?;
    if let Some(expected) = expected
        && written != expected
    {
        bail!("truncated download: got {written} bytes, expected {expected}");
    }
    Ok(ManifestEntry {
        file,
        url: url.to_owned(),
        size: written,
        sha256: hex::encode(hasher.finalize()),
        last_modified,
        etag,
        fetched_at: unix_now(),
    })
}

async fn cmd_fetch(
    jobs: NonZeroUsize,
    range: DateRange,
    dirs: DataDirOpt,
    refresh: bool,
) -> Result<()> {
    let client = build_client()?;
    let urls = load_or_fetch_listing(&client, &dirs, refresh).await?;
    let manifest = load_manifest(&dirs)?;
    let raw = dirs.raw();
    tokio::fs::create_dir_all(&raw).await?;

    let mut pending = Vec::new();
    let mut skipped = 0usize;
    for url in &urls {
        let file = filename_of(url)?;
        if !range.contains(date_of(file)?) {
            continue;
        }
        let dest = raw.join(file);
        if matches!(
            check_file(&dest, manifest.get(file), false)?,
            FileCheck::Ok { .. }
        ) {
            skipped += 1;
        } else {
            pending.push((url.clone(), dest));
        }
    }
    eprintln!(
        "{} file(s) in range: {skipped} already mirrored, {} to fetch ({jobs} connections)",
        skipped + pending.len(),
        pending.len()
    );
    if pending.is_empty() {
        return Ok(());
    }

    let manifest_file = std::sync::Mutex::new(
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dirs.manifest())?,
    );
    let mut failures = 0usize;
    for_each_parallel(
        jobs,
        pending,
        |(url, dest)| {
            let client = &client;
            async move {
                let result = download_one(client, &url, &dest).await;
                ((url, dest), result)
            }
        },
        |(url, _dest), result, bar| {
            match result {
                Ok(entry) => {
                    bar.set_message(entry.file.clone());
                    use std::io::Write;
                    let mut f = manifest_file.lock().unwrap();
                    writeln!(f, "{}", serde_json::to_string(&entry)?)?;
                }
                Err(e) => {
                    bar.println(format!("FAILED {url}: {e:#}"));
                    failures += 1;
                }
            }
            Ok(())
        },
    )
    .await?;

    if failures > 0 {
        bail!("{failures} file(s) failed to download; re-run fetch to retry");
    }
    eprintln!("all files fetched and verified against Content-Length");
    Ok(())
}

/// Compress one file with the system zstd; returns (raw bytes, compressed bytes).
async fn compress_one(threads: u32, src: &Path, dest: &Path) -> Result<(u64, u64)> {
    let tmp = dest.with_extension("zst.part");
    let status = tokio::process::Command::new("zstd")
        .args(["-q", "-f", "--ultra", "-22"])
        .arg(format!("-T{threads}"))
        .arg(src)
        .arg("-o")
        .arg(&tmp)
        .status()
        .await
        .context("running zstd — is it installed and on PATH?")?;
    if !status.success() {
        let _ = tokio::fs::remove_file(&tmp).await;
        bail!("zstd exited with {status}");
    }
    tokio::fs::rename(&tmp, dest).await?;
    Ok((src.metadata()?.len(), dest.metadata()?.len()))
}

async fn cmd_compress(
    jobs: NonZeroUsize,
    threads: u32,
    range: DateRange,
    dirs: DataDirOpt,
) -> Result<()> {
    let raw = dirs.raw();
    let out_dir = dirs.compressed();
    tokio::fs::create_dir_all(&out_dir).await?;

    let mut files: Vec<PathBuf> = std::fs::read_dir(&raw)
        .with_context(|| format!("no mirror at {} — run `fetch` first", raw.display()))?
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().ends_with("_dossier.json"))
        })
        .collect();
    files.sort();

    let mut pending = Vec::new();
    let mut skipped = 0usize;
    for src in files {
        let name = src.file_name().unwrap().to_string_lossy().into_owned();
        if !range.contains(date_of(&name)?) {
            continue;
        }
        let dest = out_dir.join(format!("{name}.zst"));
        // Compressed outputs aren't manifest-tracked; non-empty means done.
        if dest.metadata().is_ok_and(|m| m.len() > 0) {
            skipped += 1;
        } else {
            pending.push((src, dest));
        }
    }
    eprintln!(
        "{} file(s) in range: {skipped} already compressed, {} to compress \
         ({jobs} parallel zstd --ultra -22 -T{threads})",
        skipped + pending.len(),
        pending.len()
    );
    if pending.is_empty() {
        return Ok(());
    }

    let mut raw_bytes = 0u64;
    let mut zst_bytes = 0u64;
    let mut failures = 0usize;
    for_each_parallel(
        jobs,
        pending,
        |(src, dest)| async move {
            let result = compress_one(threads, &src, &dest).await;
            ((src, dest), result)
        },
        |(src, dest), result, bar| {
            match result {
                Ok((raw, zst)) => {
                    raw_bytes += raw;
                    zst_bytes += zst;
                    bar.set_message(dest.file_name().unwrap().to_string_lossy().into_owned());
                }
                Err(e) => {
                    bar.println(format!("FAILED {}: {e:#}", src.display()));
                    failures += 1;
                }
            }
            Ok(())
        },
    )
    .await?;

    if zst_bytes > 0 {
        eprintln!(
            "compressed {:.2} GB → {:.2} GB ({:.1}x)",
            raw_bytes as f64 / 1e9,
            zst_bytes as f64 / 1e9,
            raw_bytes as f64 / zst_bytes as f64,
        );
    }
    if failures > 0 {
        bail!("{failures} file(s) failed to compress; re-run to retry");
    }
    Ok(())
}

async fn cmd_verify(dirs: DataDirOpt, hash: bool) -> Result<()> {
    let listing = {
        let path = dirs.listing();
        let body = std::fs::read_to_string(&path)
            .with_context(|| format!("no listing at {} — run `list` first", path.display()))?;
        body.lines().map(str::to_owned).collect::<Vec<_>>()
    };
    let manifest = load_manifest(&dirs)?;
    let raw = dirs.raw();

    let mut missing = 0usize;
    let mut unrecorded = 0usize;
    let mut size_mismatch = 0usize;
    let mut hash_mismatch = 0usize;
    let mut ok = 0usize;
    let mut total_bytes = 0u64;

    for url in &listing {
        let file = filename_of(url)?;
        match check_file(&raw.join(file), manifest.get(file), hash)? {
            FileCheck::Missing => {
                println!("MISSING   {file}");
                missing += 1;
            }
            FileCheck::Unrecorded { size } => {
                total_bytes += size;
                println!("NO-RECORD {file} (on disk but not in manifest)");
                unrecorded += 1;
            }
            FileCheck::SizeMismatch { disk, manifest } => {
                total_bytes += disk;
                println!("BAD-SIZE  {file}: disk {disk} vs manifest {manifest}");
                size_mismatch += 1;
            }
            FileCheck::HashMismatch { size } => {
                total_bytes += size;
                println!("BAD-HASH  {file}");
                hash_mismatch += 1;
            }
            FileCheck::Ok { size } => {
                total_bytes += size;
                ok += 1;
            }
        }
    }

    // Files on disk that the listing no longer mentions (informational only).
    let listed: std::collections::HashSet<&str> =
        listing.iter().filter_map(|u| filename_of(u).ok()).collect();
    if raw.exists() {
        for dirent in std::fs::read_dir(&raw)? {
            let name = dirent?.file_name();
            let name = name.to_string_lossy();
            if !name.ends_with(".json") {
                continue;
            }
            if !listed.contains(name.as_ref()) {
                println!("EXTRA     {name} (on disk, not in listing)");
            }
        }
    }

    println!(
        "\n{ok}/{} ok ({:.2} GB on disk), {missing} missing, {unrecorded} unrecorded, \
         {size_mismatch} size mismatches, {hash_mismatch} hash mismatches",
        listing.len(),
        total_bytes as f64 / 1e9,
    );
    if missing + size_mismatch + hash_mismatch > 0 {
        bail!("mirror incomplete or corrupt — re-run fetch");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::List { dirs } => {
            let client = build_client()?;
            let urls = load_or_fetch_listing(&client, &dirs, true).await?;
            let dates: Vec<u32> = urls
                .iter()
                .map(|u| date_of(filename_of(u)?))
                .collect::<Result<_>>()?;
            eprintln!(
                "{} files, {} → {}, saved to {}",
                urls.len(),
                dates.iter().min().unwrap_or(&0),
                dates.iter().max().unwrap_or(&0),
                dirs.listing().display()
            );
            Ok(())
        }
        Cmd::Fetch {
            jobs,
            range,
            dirs,
            refresh,
        } => cmd_fetch(jobs, range, dirs, refresh).await,
        Cmd::Compress {
            jobs,
            threads,
            range,
            dirs,
        } => cmd_compress(jobs, threads, range, dirs).await,
        Cmd::Verify { dirs, hash } => cmd_verify(dirs, hash).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_listing_host() {
        assert_eq!(
            rewrite_host("https://dsn.tm.kit.edu/bitcoin/snapshots/20150715_123923_dossier.json"),
            "https://www.dsn.kastel.kit.edu/bitcoin/snapshots/20150715_123923_dossier.json"
        );
        // Already-canonical URLs pass through unchanged.
        let canonical = "https://www.dsn.kastel.kit.edu/bitcoin/snapshots/x.json";
        assert_eq!(rewrite_host(canonical), canonical);
    }

    #[test]
    fn extracts_filename_and_date() {
        let url = "https://www.dsn.kastel.kit.edu/bitcoin/snapshots/20241128_120000_dossier.json";
        let file = filename_of(url).unwrap();
        assert_eq!(file, "20241128_120000_dossier.json");
        assert_eq!(date_of(file).unwrap(), 20241128);
    }

    #[test]
    fn rejects_bad_filenames() {
        assert!(filename_of("https://example.com/").is_err());
        assert!(date_of("short").is_err());
        assert!(date_of("notadate_dossier.json").is_err());
        assert!(date_of("00001111_x.json").is_err());
    }

    fn range(from: Option<u32>, to: Option<u32>) -> DateRange {
        DateRange { from, to }
    }

    #[test]
    fn range_filter() {
        assert!(range(None, None).contains(20200101));
        assert!(range(Some(20200101), Some(20200101)).contains(20200101));
        assert!(!range(Some(20200102), None).contains(20200101));
        assert!(!range(None, Some(20191231)).contains(20200101));
    }

    #[test]
    fn file_checks() {
        let dir = std::env::temp_dir().join(format!("kit-dsn-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f.json");
        let content = b"hello world";
        std::fs::write(&path, content).unwrap();
        let entry = |size: u64, sha256: &str| ManifestEntry {
            file: "f.json".into(),
            url: "http://x/f.json".into(),
            size,
            sha256: sha256.into(),
            last_modified: None,
            etag: None,
            fetched_at: 0,
        };
        let good_sha = hex::encode(Sha256::digest(content));

        assert!(matches!(
            check_file(&dir.join("absent.json"), None, false).unwrap(),
            FileCheck::Missing
        ));
        assert!(matches!(
            check_file(&path, None, false).unwrap(),
            FileCheck::Unrecorded { size: 11 }
        ));
        assert!(matches!(
            check_file(&path, Some(&entry(99, &good_sha)), false).unwrap(),
            FileCheck::SizeMismatch {
                disk: 11,
                manifest: 99
            }
        ));
        assert!(matches!(
            check_file(&path, Some(&entry(11, &good_sha)), true).unwrap(),
            FileCheck::Ok { size: 11 }
        ));
        assert!(matches!(
            check_file(&path, Some(&entry(11, "deadbeef")), true).unwrap(),
            FileCheck::HashMismatch { size: 11 }
        ));
        // Without hash checking, a wrong recorded hash still passes on size alone.
        assert!(matches!(
            check_file(&path, Some(&entry(11, "deadbeef")), false).unwrap(),
            FileCheck::Ok { size: 11 }
        ));

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
