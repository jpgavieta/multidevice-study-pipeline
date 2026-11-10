Grzesik and Mrozek (ICCS 2020) compared TimescaleDB, InfluxDB, Riak TS, PostgreSQL and SQLite on edge hardware. PostgreSQL was fastest on inserts, and PostgreSQL and InfluxDB were the strongest on reads. They also found that a Raspberry Pi can work as a small-scale data aggregation node. This is the closest match to your low-powered server question. 
springer
nih
SciTS (Mostafa et al., SSDBM 2022) is a larger-scale benchmark. It tests ingestion and queries on ClickHouse, InfluxDB, TimescaleDB and PostgreSQL. 
arxiv

Self-hostable research platforms for wearables

RADAR-base (Ranjan et al., JMIR mHealth uHealth 2019). It is an Apache-2-licensed platform, packaged in Docker containers and built on Kafka, that can run locally or as a remote deployment. Rashid et al. (JMIR Mental Health 2024) is the follow-up on its architecture and the studies that used it. It is a research-grade reference, but Kafka makes it heavy for your hardware. 
nih
jmir

Community and citizen-science sensing infrastructure

Horsburgh et al. (Frontiers in Earth Science, 2019), “Low-Cost, Open-Source, and Low-Power: But What to Do With the Data?” It describes an open-source web portal and data management system built for a community of citizen scientists. I’d read this one first. 
frontiersin
AirKit (Sensors, 2021). It presents an open-source, end-to-end citizen-sensing infrastructure for air quality. 
core
Feenstra et al. (2020), AirSensor. It is an open-source R package and web app for interpreting community low-cost sensor data. 
aqmd

Self-hosting and who owns the data

Citizen Sense, “From the cloud to the cupboard”. It documents setting up a self-hosted cloud for a citizen-sensing project, as a response to the politics of cloud storage. 
citizensense
Malard-Adam et al. (EGU 2024 abstract). It says the cost and expertise of running servers is a major barrier for community-led projects, and that centralising data on project servers raises sovereignty questions. They explore distributed databases as an answer. It is an abstract only. 
copernicus
Cooper et al., “Equitable Data Governance Models for the Participatory Sciences” (Community Science, 2023). It notes that whoever hosts the data usually also controls how it is used, which can protect participants or be extractive. 
cas
Woo (MIT M.Eng. thesis), CommunAir. It concludes that minimal technical complexity and cost matter most for community-owned infrastructure, and it warns of a dependency risk with free cloud tools. 
mit

For more, search for “research data management” with “citizen science”, “participatory sensing data infrastructure”, and “edge database Raspberry Pi time series”. Your project is also a candidate for a short technical note, because the registry with participant, device and account windows, the raw-ingest audit log and the coverage-based backfill are things I didn’t find described elsewhere.