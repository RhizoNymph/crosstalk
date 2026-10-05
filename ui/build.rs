//! Renders the Tailwind stylesheet from the classes used in `src/`.

const TAILWIND: &str = "4.3.3";

fn main() {
    let config = topcoat::tailwind::BuildConfig::new().input("styles/app.css");
    // The checksum is of the linux-x64 binary; other hosts pin the version only.
    let config = if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        config.version_checksum(
            TAILWIND,
            "sha256:dc61b3ac6b8c9ca874c0cc4c57b2409791a64c5540404ca5f5367360babc313a",
        )
    } else {
        config.version(TAILWIND)
    };
    if let Err(err) = config.render() {
        println!("cargo::error=tailwind failed: {err}");
    }
}
