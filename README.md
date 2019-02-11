# Spark-Hotspot-Analysis

Hotspot analysis using the getis-ord statistic and Apache Spark. The Data come from AIS beacon pings of ships in the mediteranean and particularly in the aegean region. The goal is to find the statistically significant cells on the constructed grid in order to predict the existance of a port or a similar structure.
![Imis](https://github.com/Giannoulo/Spark-Hotspot-Analysis/edit/master/imis.PNG "Imis")
The size of the data is 9gb and the cluster setup on which Spark was deployed was:

1. Master1 8 CPU cores, 8Gb RAM και 60Gb Disk
2. Slave1 4 CPU cores, 8Gb RAM και 60Gb Disk
3. Slave2 4 CPU cores, 8Gb RAM και 60Gb Disk

Results:
![Greece Region](https://github.com/Giannoulo/Spark-Hotspot-Analysis/edit/master/all.PNG "Greece Region")
![Attica Region](https://github.com/Giannoulo/Spark-Hotspot-Analysis/edit/master/attica.PNG "Attica Region")
