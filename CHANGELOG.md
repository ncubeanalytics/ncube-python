# Changelog

## 0.6.0 - 2024-10-07

### Added

* Set User-Agent http headers
* Support `http_retries=-1` for maxint retries
* Use ingest base url for local validation's schema endpoint unless schema base url is explicitly passed

### Changed

* The `ingest_service_url` should no longer include the `/ingest` endpoint

## 0.4.0 - 2024-03-28

### Added

* Don't limit requests version for python 3
* Support bypassing ssl certificate verification

### Fixed

* Fix queue import for python 3.6
