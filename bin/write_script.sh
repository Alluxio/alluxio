for (( i=16; i<=1048576; i*=2 ))
do
  ./tachyon/bin/succinct runTest WriteData /data_$i 1048576 $i
done
