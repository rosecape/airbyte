import json
import snakecase

AGGREGATED_REPORT_LIST = ['AgentDurationsReport', 'AgentSummaryV2', 'AgentSummaryGlanceReportV2', 'AgentTimestampsReport', 'PaymentsByAgentReport', 'WorkSessionEventsReportV3', 'WorkSessionsReport', 'WorkSessionsReportV3', 'ContactExportReport', 'ContactExportReportV2', 'ContactExportReportV3', 'ContactSummaryDurationsReportV2', 'ContactTimestampsReport', 'ConversationExportReport', 'ConversationTimestampsReport', 'IVRExecutiveSummaryReportV2', 'IvrEndStatesReportV2', 'TaskExportReport', 'TaskTimestampsReport', 'AutoThrottleChangesReport', 'ChatDisplayPctChangesReport', 'HelpCenterAnswerSearchReport', 'SelfServiceThreadsReport', 'SidekickAnswerSearchReport', 'SidekickContactPointsReport']

# For each report, create a file in source_gladly/schemas

for report in AGGREGATED_REPORT_LIST:
    snake_case = snakecase.convert(report)
    with open(f"source_gladly/schemas/{snake_case}.json", "w") as f:
        data = json.dumps({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {}
        }, indent=4)
        f.write(data)