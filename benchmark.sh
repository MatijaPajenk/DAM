#!/usr/bin/bash

set -e

BINARY="./build/DAM"
OUTPUT_LOG="benchmark_results.txt"

if [ ! -f "$BINARY" ]; then
    echo "Binary not found at $BINARY. Building first..."
    cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
fi

echo "Running benchmark..."
echo "==============================" | tee "$OUTPUT_LOG"
echo "Benchmark: $(date)" | tee -a "$OUTPUT_LOG"
echo "Binary: $BINARY" | tee -a "$OUTPUT_LOG"
echo "==============================" | tee -a "$OUTPUT_LOG"

# Track peak memory via /proc in background
PEAK_MEM=0
PID_FILE=$(mktemp)

monitor_memory() {
    local pid=$1
    local peak=0
    while kill -0 "$pid" 2>/dev/null; do
        if [ -f "/proc/$pid/status" ]; then
            local mem
            mem=$(grep VmRSS "/proc/$pid/status" 2>/dev/null | awk '{print $2}')
            if [ -n "$mem" ] && [ "$mem" -gt "$peak" ]; then
                peak=$mem
            fi
        fi
        sleep 0.05
    done
    echo "$peak" > "$PID_FILE"
}

START_TIME=$(date +%s%N)

"$BINARY" &
PROG_PID=$!

monitor_memory "$PROG_PID" &
MONITOR_PID=$!

wait "$PROG_PID"
EXIT_CODE=$?

wait "$MONITOR_PID"

END_TIME=$(date +%s%N)

ELAPSED_MS=$(( (END_TIME - START_TIME) / 1000000 ))
ELAPSED_S=$(echo "scale=3; $ELAPSED_MS / 1000" | bc)

PEAK_KB=$(cat "$PID_FILE")
PEAK_MB=$(echo "scale=2; $PEAK_KB / 1024" | bc)

rm -f "$PID_FILE"

echo "Exit code:    $EXIT_CODE"        | tee -a "$OUTPUT_LOG"
echo "Elapsed time: ${ELAPSED_S}s"     | tee -a "$OUTPUT_LOG"
echo "Peak RSS:     ${PEAK_MB} MB (${PEAK_KB} kB)" | tee -a "$OUTPUT_LOG"

echo "" | tee -a "$OUTPUT_LOG"
echo "Output files:" | tee -a "$OUTPUT_LOG"
for dir in data/sorted data/tree; do
    if [ -d "$dir" ]; then
        COUNT=$(find "$dir" -type f | wc -l)
        TOTAL_SIZE=$(du -sh "$dir" 2>/dev/null | cut -f1)
        echo "  $dir: $COUNT files, $TOTAL_SIZE total" | tee -a "$OUTPUT_LOG"
        find "$dir" -type f -name "*.csv" | sort | while read -r f; do
            SIZE=$(du -sh "$f" | cut -f1)
            echo "    $f ($SIZE)" | tee -a "$OUTPUT_LOG"
        done
    fi
done

echo "==============================" | tee -a "$OUTPUT_LOG"
echo "Results saved to $OUTPUT_LOG"