# Log Settings
LOG = {
    "filters": {
        "masking": {
            "rules": {
                "Collector.verify": ["secret_data"],
                "Collector.collect": ["secret_data"],
                "Job.get_tasks": ["secret_data"],
            }
        }
    }
}

# Cache Settings
CACHES = {
    "local": {
        "backend": "spaceone.core.cache.local_cache.LocalCache",
        "max_size": 128,
        "ttl": 86400,
    },
}
