# retentionAnalyzer
Parse Icinga retention.dat file and send metrics to InfluxDB

Example of use:
```
retentionAnalyzer.py -vv -e someEnv -i 127.0.0.1 -u USER -p PASSWORD -d mydatabase -r retention.dat
```

I have found it useful to see how execution of the checks are distributed.
