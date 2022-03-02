
runtest() {
    pytest -s --log-cli-level=DEBUG --asyncio-mode=auto test
}

samplegen() {
    rm -r samples && mkdir samples
    python jkolyer/directories.py --tree_depth 3 samples    
}

flush_db() {
    python jkolyer/jkolyer.py --flush_db
}

