#!/bin/bash
> logs/input1.log
> logs/input2.log
> logs/arp.log
#lein jar
#java -cp target/arp-0.1.3.jar:lib/arp-0.1.3-standalone.jar mx.interware.arp.core.Starter config/arp-config.edn
lein run config/arp-config.edn
