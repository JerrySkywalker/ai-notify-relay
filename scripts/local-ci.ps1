$ErrorActionPreference = 'Stop'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    cargo fmt --all --check
    if ($LASTEXITCODE) { throw 'cargo fmt failed' }
    cargo clippy --locked --all-targets -- -D warnings
    if ($LASTEXITCODE) { throw 'cargo clippy failed' }
    cargo test --locked
    if ($LASTEXITCODE) { throw 'cargo test failed' }
    cargo build --locked --release
    if ($LASTEXITCODE) { throw 'cargo build failed' }
} finally {
    Pop-Location
}
