# Distributed Analytics Platform (Spark + Terraform)

A production-grade distributed batch and analytics platform processing 20.33 million records (~3.5GB in-memory footprint) from the NYC TLC Trip Record dataset. Engineered to demonstrate distributed data engineering patterns—partition pruning, map-side broadcast joins, shuffle tuning, and analytical window functions—paired with Infrastructure as Code (IaC) via Terraform.

## Project Status:
- [x] **Phase 1: Data Acquisition & Landing Zone** (20.33M rows ingested, schema validation across 6 consecutive months)
- [x] **Phase 2: Spark Transform & Partitioning Engine** (broadcast joins, 7-day rolling window analytics, borough/date partitioning)
- [x] **Phase 3: Formal Testing & Validation Suite** (chispa/pytest data contracts)
- [ ] **Phase 4: Analytics Dashboard** (Streamlit zone metrics & heatmap)
- [ ] **Phase 5: Terraform Infrastructure as Code** (S3/IAM provisioning with CI plan checks)
- [ ] **Phase 6: Production Scaling Blueprint & Case Study**

*Note: Comprehensive system architecture and recruiter-facing documentation will be finalized in Phase 6.*
