use rspdk::benchmark;

fn main() {
    benchmark(64 * 1024, 50);
}
