for ((i=0; i<=8; i++))
    do
    cargo test -- test_persist2_2c --nocapture > out--$i &
done