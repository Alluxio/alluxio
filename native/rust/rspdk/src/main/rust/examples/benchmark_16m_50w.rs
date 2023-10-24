use rspdk::benchmark;

fn main() {
    benchmark(16 * 1024 * 1024, 50);
}
