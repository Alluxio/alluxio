for (( i=512; i<=1048576; i*=2 ))
do
  echo "i = $1"
  cat log_$i | grep 'Throughput' | awk '{ print($3) }' >> thputs
done
