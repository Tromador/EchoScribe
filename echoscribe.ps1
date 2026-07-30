$ErrorActionPreference = "Stop"

$manifestPath = Join-Path $PSScriptRoot "Cargo.toml"
& cargo run --release --manifest-path $manifestPath -- @args
exit $LASTEXITCODE
