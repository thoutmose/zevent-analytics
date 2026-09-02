#!/bin/sh -e
# Tightens the provenance repository's retention before the stock entrypoint starts
# NiFi. Left at image defaults (30 days / 10 GB) this repo is pure operational
# audit-trail data -- lineage events for every flowfile, not archived pipeline
# output -- so nothing in the compress/archive process ever touches it. At
# Zevent-scale ingest volume (one event per chat message) it was on track to
# quietly eat 10 GB of srv-prod's disk. See ARCHITECTURE.md.
#
# Also applies the content repository archive settings from ARCHITECTURE.md's
# "Tuning for higher throughput" stress test (max.retention.period 7 days -> 2 mins,
# max.usage.percentage 50% -> 90%): these directly gate how aggressively NiFi
# throttles writes under disk pressure, and were previously only ever applied live
# via the NiFi API/UI -- meaning they silently reverted to the image defaults on
# every container recreate, since nifi.properties isn't a mounted volume.
# nifi.content.repository.archive.enabled is already "true" at the image default --
# set explicitly here so it can't silently drift, since it's the master switch the
# two properties above depend on.
#
# nifi.queue.swap.threshold governs when a connection's queued flowfile records get
# paged out of heap to disk -- README's "Stability findings" measured this image's
# default at 10,000 flowfiles (that's where the "drops from ~3,000 rows/s to ~60
# rows/s" swap-churn finding kicks in), not the 20,000 Apache's own docs describe for
# stock nifi.properties, so this comment trusts the repo's own measurement over the
# upstream default. Left at that default, every one of this flow's connections -- now
# backpressured at 500,000 objects (see flow-templates/zevent-ingest-flow.json,
# ARCHITECTURE.md's "Tuning for higher throughput") -- spends most of its normal
# burst-absorption range already swapping, not just its worst case. Raised to 200,000:
# comfortably above where routine bursts land (per the same stress test) so swapping
# isn't constant, still well under the 500,000 backpressure cap so heap stays bounded
# before backpressure kicks in. This number is reasoned from the existing stress-test
# numbers, not itself stress-tested -- validate it the same way (README's
# "Performance" section) before trusting it on srv-prod.
sed -i \
  -e 's|^nifi.provenance.repository.max.storage.time=.*$|nifi.provenance.repository.max.storage.time=3 days|' \
  -e 's|^nifi.provenance.repository.max.storage.size=.*$|nifi.provenance.repository.max.storage.size=1 GB|' \
  -e 's|^nifi.content.repository.archive.max.retention.period=.*$|nifi.content.repository.archive.max.retention.period=2 mins|' \
  -e 's|^nifi.content.repository.archive.max.usage.percentage=.*$|nifi.content.repository.archive.max.usage.percentage=90%|' \
  -e 's|^nifi.content.repository.archive.enabled=.*$|nifi.content.repository.archive.enabled=true|' \
  -e 's|^nifi.queue.swap.threshold=.*$|nifi.queue.swap.threshold=200000|' \
  /opt/nifi/nifi-current/conf/nifi.properties

exec /opt/nifi/scripts/start.sh
