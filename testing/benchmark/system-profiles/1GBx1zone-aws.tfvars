user_name = "USER"

# APM bench

worker_instance_type = "c6i.large"

# Elastic Cloud
ess_region          = "aws-eu-west-1"
deployment_template = "aws-cpu-optimized-faster-warm-arm"
# The number of AZs the APM Server should span.
apm_server_zone_count = 1
# The Elasticsearch cluster node size.
elasticsearch_size = "15g"
# The number of AZs the Elasticsearch cluster should have.
elasticsearch_zone_count = 2
# APM server instance size
apm_server_size = "1g"

# Standalone

standalone_apm_server_instance_size = "c6i.large"
standalone_moxy_instance_size       = "c6i.xlarge"
