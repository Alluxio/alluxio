use rspdk::benchmark;

fn main() {
    benchmark(4 * 1024, 50);
}
