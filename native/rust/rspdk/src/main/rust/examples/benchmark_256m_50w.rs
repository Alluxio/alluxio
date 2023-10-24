use rspdk::benchmark;

fn main() {
    benchmark(256 * 1024 * 1024, 50);
}
