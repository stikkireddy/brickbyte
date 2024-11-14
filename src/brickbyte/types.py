from typing import Literal

Source = Literal[
    "source-adjust",
    "source-airtable",
    "source-alpha-vantage",
    "source-amazon-ads",
    "source-amazon-seller-partner",
    "source-amplitude",
    "source-appsflyer",
    "source-asana",
    "source-avni",
    "source-azure-blob-storage",
    "source-azure-table",
    "source-bing-ads",
    "source-braintree",
    "source-cart",
    "source-chargebee",
    "source-close-com",
    "source-commcare",
    "source-commercetools",
    "source-convex",
    "source-declarative-manifest",
    "source-facebook-marketing",
    "source-facebook-pages",
    "source-faker",
    "source-file",
    "source-firebase-realtime-database",
    "source-firebolt",
    "source-freshcaller",
    "source-freshdesk",
    "source-gcs",
    "source-genesys",
    "source-github",
    "source-gitlab",
    "source-google-ads",
    "source-google-analytics-data-api",
    "source-google-analytics-v4",
    "source-google-directory",
    "source-google-drive",
    "source-google-search-console",
    "source-google-sheets",
    "source-greenhouse",
    "source-gridly",
    "source-hardcoded-records",
    "source-harness",
    "source-hubspot",
    "source-instagram",
    "source-instatus",
    "source-intercom",
    "source-iterable",
    "source-jina-ai-reader",
    "source-jira",
    "source-klaviyo",
    "source-kyriba",
    "source-kyve",
    "source-linkedin-ads",
    "source-linnworks",
    "source-looker",
    "source-mailchimp",
    "source-marketo",
    "source-microsoft-dataverse",
    "source-microsoft-onedrive",
    "source-microsoft-sharepoint",
    "source-mixpanel",
    "source-monday",
    "source-notion",
    "source-okta",
    "source-outbrain-amplify",
    "source-outreach",
    "source-pagerduty",
    "source-partnerstack",
    "source-paypal-transaction",
    "source-pinterest",
    "source-pipedrive",
    "source-posthog",
    "source-prestashop",
    "source-public-apis",
    "source-quickbooks",
    "source-railz",
    "source-recharge",
    "source-rki-covid",
    "source-rss",
    "source-s3",
    "source-salesforce",
    "source-sentry",
    "source-sftp-bulk",
    "source-shopify",
    "source-slack",
    "source-smartsheets",
    "source-stripe",
    "source-surveycto",
    "source-surveymonkey",
    "source-tiktok-marketing",
    "source-tplcentral",
    "source-twilio",
    "source-typeform",
    "source-webflow",
    "source-xero",
    "source-yandex-metrica",
    "source-youtube-analytics",
    "source-zendesk-chat",
    "source-zendesk-support",
    "source-zenloop"
]


Destination = Literal["destination-databricks"]