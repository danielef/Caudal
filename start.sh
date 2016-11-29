#!/bin/bash
lein jar
java -cp target/arp-0.1.3.jar:lib/arp-0.1.3-standalone.jar mx.interware.arp.core.Starter config/arp-config.edn
lein run config/arp-config.edn
