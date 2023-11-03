# Rust Binding for SPDK

## usage

in spdk directory
```shell
cp dpdk/build/lib/*.a build/lib/

cp isa-l/.libs/*.a build/lib/

cp isa-l-crypto/.libs/*.a build/lib/

cd build/lib/

rm libspdk_ut_mock.a

cc -shared -o libspdk_fat.so -Wl,--whole-archive *.a -Wl,--no-whole-archive

sudo cp libspdk_fat.so /usr/local/lib
```
