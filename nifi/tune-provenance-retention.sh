#!/bin/sh -e
# Tightens the provenance repository's retention before the stock entrypoint starts
# NiFi. Left at image defaults (30 days / 10 GB) this repo is pure operational
# audit-trail data -- lineage events for every flowfile, not archived pipeline
# output -- so nothing in the compress/archive process ever touches it. At
# Zevent-scale ingest volume (one event per chat message) it was on track to
# quietly eat 10 GB of srv-prod's disk. See ARCHITECTURE.md.
sed -i \
  -e 's|^nifi.provenance.repository.max.storage.time=.*$|nifi.provenance.repository.max.storage.time=3 days|' \
  -e 's|^nifi.provenance.repository.max.storage.size=.*$|nifi.provenance.repository.max.storage.size=1 GB|' \
  /opt/nifi/nifi-current/conf/nifi.properties

exec /opt/nifi/scripts/start.sh
