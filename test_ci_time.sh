#!/bin/bash

echo "=== GitHub Actions CI Süre Testi ==="
echo ""

echo "1️⃣  Format Check"
time cargo fmt --all -- --check
echo ""

echo "2️⃣  Clippy"
time cargo clippy --workspace --all-targets --all-features -- -D warnings
echo ""

echo "3️⃣  Tests"
time cargo test --workspace --all-features
echo ""

echo "4️⃣  Release Build"
time cargo build --workspace --all-features --release
echo ""

echo "✅ Tüm CI adımları başarılı!"
