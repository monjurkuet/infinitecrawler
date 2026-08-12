"""api/services/pg_service.py — Thin re-export shim for the split services.

This module was split into 8 domain modules in 2026-08-12 (B1 refactor).
Callers can still use `pg_service.<fn>`; the functions below are marked
deprecated and should be migrated to direct module imports over the next
release cycle.

  pool:        api.services.pg_pool
  leads:       api.services.leads_repo
  search:      api.services.search_repo
  enrichment:  api.services.enrichment_repo
  dashboard:   api.services.dashboard_repo
  tasks:       api.services.tasks_repo
  luxury:      api.services.luxury_repo
  exports:     api.services.exports
"""

from api.services.dashboard_repo import (  # noqa: F401  # DEPRECATED: import from dashboard_repo
    get_coverage,
    get_dashboard_overview,
    get_recent_activity,
    get_throughput,
    get_total_throughput_24h,
)
from api.services.enrichment_repo import (  # noqa: F401  # DEPRECATED: import from enrichment_repo
    get_classification_stats,
    get_email_stats,
    get_emails_by_listing,
    get_linkedin_by_listing,
    get_linkedin_stats,
    query_emails,
    query_linkedin_profiles,
    query_unclassified,
)
from api.services.exports import export_leads_csv  # noqa: F401  # DEPRECATED: import from exports
from api.services.leads_repo import (  # noqa: F401  # DEPRECATED: import from leads_repo
    _build_leads_where,
    get_lead_by_id,
    get_lead_stats,
    get_leads_by_city,
    get_leads_by_sector,
    query_leads,
)
from api.services.luxury_repo import (  # noqa: F401  # DEPRECATED: import from luxury_repo
    get_luxury_stats,
    query_luxury_contacts,
    query_luxury_targets,
)
from api.services.pg_pool import (  # noqa: F401  # DEPRECATED: import from pg_pool
    check_health,
    close_pool,
    create_pool,
    get_pool,
)
from api.services.search_repo import (  # noqa: F401  # DEPRECATED: import from search_repo
    get_search_result_by_id,
    get_search_result_stats,
    get_uncrawled_count,
    query_search_results,
)
from api.services.tasks_repo import (  # noqa: F401  # DEPRECATED: import from tasks_repo
    TASKS_SCHEMA,
    TASKS_TABLE,
    cancel_pipeline_task,
    create_pipeline_task,
    ensure_tasks_table,
    get_active_pipeline_tasks,
    get_task,
    list_pipeline_tasks,
    list_tasks,
    save_task,
    update_task_status,
)
