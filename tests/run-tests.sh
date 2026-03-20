for i in test_*.py; do
    echo $i
    pytest -s $i
    echo DONE $i
done