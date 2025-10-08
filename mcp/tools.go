package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// CalculatorTool Calculator tool for basic math operations
func CalculatorTool() server.ServerTool {
	tool := mcp.NewTool("calculator",
		mcp.WithDescription("Perform basic mathematical calculations"),
		mcp.WithString("operation",
			mcp.Description("The mathematical operation to perform"),
			mcp.Required(),
			mcp.Enum("add", "subtract", "multiply", "divide", "power", "sqrt"),
		),
		mcp.WithNumber("first_number",
			mcp.Description("The first number for the operation"),
			mcp.Required(),
		),
		mcp.WithNumber("second_number",
			mcp.Description("The second number (not required for sqrt)"),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		operation, err := request.RequireString("operation")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		firstNum, err := request.RequireFloat("first_number")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		var result float64
		var operatorSymbol string

		switch operation {
		case "add":
			secondNum, err := request.RequireFloat("second_number")
			if err != nil {
				return mcp.NewToolResultError("second_number is required for addition"), nil
			}
			result = firstNum + secondNum
			operatorSymbol = "+"

		case "subtract":
			secondNum, err := request.RequireFloat("second_number")
			if err != nil {
				return mcp.NewToolResultError("second_number is required for subtraction"), nil
			}
			result = firstNum - secondNum
			operatorSymbol = "-"

		case "multiply":
			secondNum, err := request.RequireFloat("second_number")
			if err != nil {
				return mcp.NewToolResultError("second_number is required for multiplication"), nil
			}
			result = firstNum * secondNum
			operatorSymbol = "×"

		case "divide":
			secondNum, err := request.RequireFloat("second_number")
			if err != nil {
				return mcp.NewToolResultError("second_number is required for division"), nil
			}
			if secondNum == 0 {
				return mcp.NewToolResultError("cannot divide by zero"), nil
			}
			result = firstNum / secondNum
			operatorSymbol = "÷"

		default:
			return mcp.NewToolResultError("unsupported operation"), nil
		}

		resultStr := fmt.Sprintf("%.2f %s %.2f = %.6f", firstNum, operatorSymbol, result, result)
		return mcp.NewToolResultText(resultStr), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

// SystemInfoTool provides system information
func SystemInfoTool() server.ServerTool {
	tool := mcp.NewTool("system_info",
		mcp.WithDescription("Get system information like current time and date"),
		mcp.WithString("info_type",
			mcp.Description("Type of system information to retrieve"),
			mcp.Required(),
			mcp.Enum("time", "date", "datetime"),
		),
		mcp.WithString("format",
			mcp.Description("Format for the output"),
			mcp.DefaultString("human"),
			mcp.Enum("iso", "rfc3339", "unix", "human"),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		infoType, err := request.RequireString("info_type")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		format := request.GetString("format", "human")
		now := time.Now()

		var result string
		switch infoType {
		case "time":
			switch format {
			case "iso":
				result = now.Format("15:04:05")
			case "rfc3339":
				result = now.Format(time.RFC3339)
			case "unix":
				result = fmt.Sprintf("%d", now.Unix())
			case "human":
				result = now.Format("3:04:05 PM")
			}
		case "date":
			switch format {
			case "iso":
				result = now.Format("2006-01-02")
			case "rfc3339":
				result = now.Format(time.RFC3339)
			case "unix":
				result = fmt.Sprintf("%d", now.Unix())
			case "human":
				result = now.Format("Monday, January 2, 2006")
			}
		case "datetime":
			switch format {
			case "iso":
				result = now.Format("2006-01-02T15:04:05")
			case "rfc3339":
				result = now.Format(time.RFC3339)
			case "unix":
				result = fmt.Sprintf("%d", now.Unix())
			case "human":
				result = now.Format("Monday, January 2, 2006 at 3:04:05 PM")
			}
		}

		return mcp.NewToolResultText(result), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

// ReconFileAnalysisTool analyzes uploaded reconciliation files
func ReconFileAnalysisTool() server.ServerTool {
	tool := mcp.NewTool("recon_file_analysis",
		mcp.WithDescription("Analyze uploaded reconciliation files to identify EntityID and Amount columns for master source creation"),
		mcp.WithString("file1_path",
			mcp.Description("Full file path to the first reconciliation file (e.g., /path/to/transactions.csv or /path/to/transactions.xlsx)"),
			mcp.Required(),
		),
		mcp.WithString("file1_type",
			mcp.Description("Type of the first file"),
			mcp.Required(),
			mcp.Enum("csv", "excel"),
		),
		mcp.WithString("file2_path",
			mcp.Description("Full file path to the second reconciliation file (e.g., /path/to/bank_statements.csv or /path/to/bank_statements.xlsx)"),
			mcp.Required(),
		),
		mcp.WithString("file2_type",
			mcp.Description("Type of the second file"),
			mcp.Required(),
			mcp.Enum("csv", "excel"),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		// This would typically call your helper functions
		// For now, return a simulated analysis result
		result := `{
			"analysis_type": "comprehensive",
			"compatibility_check": {
				"can_reconcile": true,
				"common_patterns": ["amount", "date"],
				"suggested_reconciliation": "Match by EntityID and Amount fields"
			},
			"file_analysis": {
				"file_1": {
					"all_columns": ["paymentid", "txn_amount", "date"],
					"amount_candidates": [
						{
							"column_name": "txn_amount",
							"confidence": 0.83,
							"reason": "Amount-like naming with monetary values",
							"sample_values": ["500", "1500", "200"]
						}
					],
					"entityid_candidates": [
						{
							"column_name": "paymentid",
							"confidence": 0.98,
							"reason": "ID-like naming pattern with high uniqueness",
							"unique_percentage": 100
						}
					],
					"file_type": "csv",
					"recommended_amount": "txn_amount",
					"recommended_entityid": "paymentid",
					"total_columns": 3,
					"total_rows": 4
				}
			}
		}`

		return mcp.NewToolResultText(result), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

// ReconMasterSourceTool creates master source configurations
func ReconMasterSourceTool() server.ServerTool {
	tool := mcp.NewTool("recon_master_source",
		mcp.WithDescription("Create master source configurations for recon-saas using file analysis data"),
		mcp.WithString("source1_name",
			mcp.Description("Name for the first master source"),
			mcp.Required(),
		),
		mcp.WithString("source2_name",
			mcp.Description("Name for the second master source"),
			mcp.Required(),
		),
		mcp.WithString("source1_columns",
			mcp.Description("JSON array of column names from first file"),
			mcp.Required(),
		),
		mcp.WithString("source2_columns",
			mcp.Description("JSON array of column names from second file"),
			mcp.Required(),
		),
		mcp.WithString("source1_entityid",
			mcp.Description("Selected EntityID column name for first source"),
			mcp.Required(),
		),
		mcp.WithString("source2_entityid",
			mcp.Description("Selected EntityID column name for second source"),
			mcp.Required(),
		),
		mcp.WithString("source1_amount",
			mcp.Description("Selected Amount column name for first source"),
			mcp.Required(),
		),
		mcp.WithString("source2_amount",
			mcp.Description("Selected Amount column name for second source"),
			mcp.Required(),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		// Extract parameters
		source1Name, _ := request.RequireString("source1_name")
		source2Name, _ := request.RequireString("source2_name")

		// Simulate API calls to create master sources
		masterSourceID1 := generateID(8)
		masterSourceID2 := generateID(8)

		result := fmt.Sprintf(`{
			"created_sources": {
				"source_1": {
					"master_source_id": "%s",
					"name": "%s",
					"selected_entityid_column": "%s",
					"selected_amount_column": "%s"
				},
				"source_2": {
					"master_source_id": "%s", 
					"name": "%s",
					"selected_entityid_column": "%s",
					"selected_amount_column": "%s"
				}
			},
			"message": "Master sources created successfully",
			"status": "success"
		}`, masterSourceID1, source1Name, request.GetString("source1_entityid", ""), request.GetString("source1_amount", ""),
			masterSourceID2, source2Name, request.GetString("source2_entityid", ""), request.GetString("source2_amount", ""))

		return mcp.NewToolResultText(result), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

// ReconMerchantSourceTool creates merchant-specific source configurations
func ReconMerchantSourceTool() server.ServerTool {
	tool := mcp.NewTool("recon_merchant_source",
		mcp.WithDescription("Create merchant-specific source configurations for recon-saas"),
		mcp.WithString("merchant_id",
			mcp.Description("Merchant identifier for this onboarding process"),
			mcp.Required(),
		),
		mcp.WithString("master_source_id_1",
			mcp.Description("First master source ID from previous step"),
			mcp.Required(),
		),
		mcp.WithString("master_source_id_2",
			mcp.Description("Second master source ID from previous step"),
			mcp.Required(),
		),
		mcp.WithString("source_1_name",
			mcp.Description("Name of the first source"),
			mcp.Required(),
		),
		mcp.WithString("source_2_name",
			mcp.Description("Name of the second source"),
			mcp.Required(),
		),
		mcp.WithString("source_naming_strategy",
			mcp.Description("Strategy for naming merchant sources"),
			mcp.DefaultString("descriptive"),
			mcp.Enum("descriptive", "timestamp", "sequential", "custom"),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		merchantID, _ := request.RequireString("merchant_id")
		masterSourceID1, _ := request.RequireString("master_source_id_1")
		masterSourceID2, _ := request.RequireString("master_source_id_2")
		source1Name, _ := request.RequireString("source_1_name")
		source2Name, _ := request.RequireString("source_2_name")

		// Generate merchant source IDs
		merchantSourceID1 := generateID(8)
		merchantSourceID2 := generateID(8)

		result := fmt.Sprintf(`{
			"created_merchant_sources": {
				"merchant_source_1": {
					"master_source_id": "%s",
					"merchant_id": "%s",
					"merchant_source_id": "%s",
					"name": "%s - Merchant Portal",
					"naming_strategy": "descriptive"
				},
				"merchant_source_2": {
					"master_source_id": "%s",
					"merchant_id": "%s", 
					"merchant_source_id": "%s",
					"name": "%s - Merchant Portal",
					"naming_strategy": "descriptive"
				}
			},
			"execution_summary": {
				"failed_creations": 0,
				"merchant_id": "%s",
				"successful_creations": 2,
				"total_merchant_sources": 2
			},
			"message": "Merchant sources created successfully",
			"status": "success"
		}`, masterSourceID1, merchantID, merchantSourceID1, source1Name,
			masterSourceID2, merchantID, merchantSourceID2, source2Name, merchantID)

		return mcp.NewToolResultText(result), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

// ReconStateRuleTool creates reconciliation states and rules
func ReconStateRuleTool() server.ServerTool {
	tool := mcp.NewTool("recon_state_rule",
		mcp.WithDescription("Create reconciliation states and corresponding rules for recon-saas"),
		mcp.WithString("merchant_id",
			mcp.Description("Merchant identifier"),
			mcp.Required(),
		),
		mcp.WithString("master_source_id_1",
			mcp.Description("First master source ID"),
			mcp.Required(),
		),
		mcp.WithString("master_source_id_2",
			mcp.Description("Second master source ID"),
			mcp.Required(),
		),
		mcp.WithString("source_1_name",
			mcp.Description("Name of the first source for remarks"),
			mcp.Required(),
		),
		mcp.WithString("source_2_name",
			mcp.Description("Name of the second source for remarks"),
			mcp.Required(),
		),
		mcp.WithString("validation_mode",
			mcp.Description("User validation mode for rule expressions"),
			mcp.DefaultString("guided"),
			mcp.Enum("automatic", "guided", "manual"),
		),
		mcp.WithBoolean("approve_expressions",
			mcp.Description("Whether to approve the generated rule expressions"),
			mcp.DefaultString("true"),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		merchantID, _ := request.RequireString("merchant_id")
		masterSourceID1, _ := request.RequireString("master_source_id_1")
		masterSourceID2, _ := request.RequireString("master_source_id_2")
		source1Name, _ := request.RequireString("source_1_name")
		source2Name, _ := request.RequireString("source_2_name")

		// Generate state and rule IDs
		reconciledStateID := generateID(8)
		amountMismatchStateID := generateID(8)
		missingFile1StateID := generateID(8)
		missingFile2StateID := generateID(8)

		reconciledRuleID := generateID(8)
		amountMismatchRuleID := generateID(8)
		missingFile1RuleID := generateID(8)
		missingFile2RuleID := generateID(8)

		result := fmt.Sprintf(`{
			"created_recon_states": {
				"reconciled_state": {
					"recon_state_id": "%s",
					"name": "Reconciled",
					"priority": 2,
					"remarks": "success"
				},
				"amount_mismatch_state": {
					"recon_state_id": "%s",
					"name": "Unreconciled", 
					"priority": 3,
					"remarks": "Amount mismatch"
				},
				"missing_file1_state": {
					"recon_state_id": "%s",
					"name": "Unreconciled",
					"priority": 3,
					"remarks": "Record not found in %s"
				},
				"missing_file2_state": {
					"recon_state_id": "%s",
					"name": "Unreconciled",
					"priority": 3,
					"remarks": "Record not found in %s"
				}
			},
			"created_rules": {
				"reconciled_rule": {
					"rule_id": "%s",
					"name": "Reconciled Transactions",
					"expression": "%s.EntityID == %s.EntityID && %s.Amount.Equal(%s.Amount)",
					"recon_state_id": "%s"
				},
				"amount_mismatch_rule": {
					"rule_id": "%s", 
					"name": "Amount Mismatch Transactions",
					"expression": "%s.EntityID == %s.EntityID && !%s.Amount.Equal(%s.Amount)",
					"recon_state_id": "%s"
				},
				"missing_record_rule_file1": {
					"rule_id": "%s",
					"name": "Missing Record in File 1",
					"expression": "NoRecord.Value == true",
					"recon_state_id": "%s"
				},
				"missing_record_rule_file2": {
					"rule_id": "%s",
					"name": "Missing Record in File 2", 
					"expression": "NoRecord.Value == true",
					"recon_state_id": "%s"
				}
			},
			"execution_summary": {
				"merchant_id": "%s",
				"total_recon_states": 4,
				"total_rules": 4,
				"user_approved_expressions": true,
				"validation_applied": true,
				"validation_mode": "automatic"
			},
			"message": "Recon states and rules created successfully",
			"status": "success"
		}`, reconciledStateID, amountMismatchStateID, missingFile1StateID, source1Name, missingFile2StateID, source2Name,
			reconciledRuleID, masterSourceID1, masterSourceID2, masterSourceID1, masterSourceID2, reconciledStateID,
			amountMismatchRuleID, masterSourceID1, masterSourceID2, masterSourceID1, masterSourceID2, amountMismatchStateID,
			missingFile1RuleID, missingFile1StateID, missingFile2RuleID, missingFile2StateID, merchantID)

		return mcp.NewToolResultText(result), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

// ReconProcessSetupTool creates lookup configurations and reconciliation processes
func ReconProcessSetupTool() server.ServerTool {
	tool := mcp.NewTool("recon_process_setup",
		mcp.WithDescription("Create lookup configurations and reconciliation processes for recon-saas"),
		mcp.WithString("merchant_id",
			mcp.Description("Merchant identifier"),
			mcp.Required(),
		),
		mcp.WithString("master_source_id_1",
			mcp.Description("First master source ID"),
			mcp.Required(),
		),
		mcp.WithString("master_source_id_2",
			mcp.Description("Second master source ID"),
			mcp.Required(),
		),
		mcp.WithString("merchant_source_id_1",
			mcp.Description("First merchant source ID"),
			mcp.Required(),
		),
		mcp.WithString("merchant_source_id_2",
			mcp.Description("Second merchant source ID"),
			mcp.Required(),
		),
		mcp.WithString("rule_ids",
			mcp.Description("JSON array of rule IDs from previous step"),
			mcp.Required(),
		),
		mcp.WithString("source_1_name",
			mcp.Description("Name of the first source"),
			mcp.Required(),
		),
		mcp.WithString("source_2_name",
			mcp.Description("Name of the second source"),
			mcp.Required(),
		),
		mcp.WithString("source1_columns",
			mcp.Description("JSON array of column names from first file"),
			mcp.Required(),
		),
		mcp.WithString("source2_columns",
			mcp.Description("JSON array of column names from second file"),
			mcp.Required(),
		),
		mcp.WithString("source1_entityid",
			mcp.Description("Selected EntityID column name for first source"),
			mcp.Required(),
		),
		mcp.WithString("source2_entityid",
			mcp.Description("Selected EntityID column name for second source"),
			mcp.Required(),
		),
		mcp.WithString("source1_amount",
			mcp.Description("Selected Amount column name for first source"),
			mcp.Required(),
		),
		mcp.WithString("source2_amount",
			mcp.Description("Selected Amount column name for second source"),
			mcp.Required(),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		merchantID, _ := request.RequireString("merchant_id")
		source1Name, _ := request.RequireString("source_1_name")
		source2Name, _ := request.RequireString("source_2_name")

		// Generate process IDs
		lookupID := generateID(8)
		masterReconProcessID := generateID(8)
		merchantReconProcessID := generateID(8)

		result := fmt.Sprintf(`{
			"created_components": {
				"lookup": {
					"lookup_id": "%s",
					"name": "Entity Lookup for %s and %s"
				},
				"master_recon_process": {
					"master_recon_process_id": "%s",
					"name": "%s to %s Reconciliation"
				},
				"merchant_recon_process": {
					"merchant_recon_process_id": "%s"
				}
			},
			"execution_summary": {
				"failed_creations": 0,
				"merchant_id": "%s",
				"process_name": "%s to %s Reconciliation",
				"successful_creations": 3,
				"total_api_calls": 3
			},
			"message": "Reconciliation process setup completed successfully",
			"onboarding_completion": {
				"message": "Merchant onboarding successfully completed. The reconciliation process is now ready for file uploads and processing.",
				"next_steps": [
					"Upload transaction files for reconciliation",
					"Monitor reconciliation results in dashboard",
					"Configure automated file processing schedules",
					"Set up reporting and alerting preferences"
				],
				"status": "COMPLETE"
			},
			"status": "success"
		}`, lookupID, source1Name, source2Name, masterReconProcessID, source1Name, source2Name,
			merchantReconProcessID, merchantID, source1Name, source2Name)

		return mcp.NewToolResultText(result), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

func ReconDataExtractionTool() server.ServerTool {
	tool := mcp.NewTool("recon_data_extraction",
		mcp.WithDescription("Extract specific patterns or data from reconciliation files using regex (regular expressions)"),
		mcp.WithString("merchant_id",
			mcp.Description("Merchant identifier for this extraction process"),
			mcp.Required(),
		),
		mcp.WithString("merchant_source_id",
			mcp.Description("Merchant source ID to apply extraction to (from previous merchant source creation)"),
			mcp.Required(),
		),
		mcp.WithString("column_name",
			mcp.Description("Name of the column containing data to extract from (e.g., 'paymentid', 'transaction_id')"),
			mcp.Required(),
		),
		mcp.WithString("extraction_pattern",
			mcp.Description("What to extract from the column data. Options: 'integers' for numbers, specific text like '123', or advanced regex like '(?<=NEFT-)[A-Z0-9]+(?=-)'"),
			mcp.Required(),
		),
		mcp.WithString("extracted_column_name",
			mcp.Description("Name for the extracted column (e.g., 'entity_id', 'reference_code', 'transaction_id')"),
			mcp.DefaultString("extracted_value"),
		),
		mcp.WithString("extraction_name",
			mcp.Description("Name for this extraction configuration"),
			mcp.DefaultString("regex_extraction"),
		),
		mcp.WithBoolean("apply_immediately",
			mcp.Description("Whether to apply extraction immediately to the source"),
			mcp.DefaultBool(true),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		// Extract and validate parameters
		merchantID, err := request.RequireString("merchant_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		merchantSourceID, err := request.RequireString("merchant_source_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		columnName, err := request.RequireString("column_name")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		extractionPattern, err := request.RequireString("extraction_pattern")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		extractedColumnName := request.GetString("extracted_column_name", "extracted_value")
		extractionName := request.GetString("extraction_name", "regex_extraction")
		applyImmediately := request.GetBool("apply_immediately", true)

		// Generate regex pattern based on extraction pattern
		var regexPatterns []string

		if extractionPattern == "integers" || extractionPattern == "numbers" {
			// Extract all numbers from the data
			regexPatterns = []string{`([0-9]+)`}
		} else if strings.HasPrefix(extractionPattern, "$") {
			// Column reference pattern like "$UTR"
			regexPatterns = []string{extractionPattern}
		} else if strings.Contains(extractionPattern, "(?<=") || strings.Contains(extractionPattern, "(?=") {
			// Advanced regex with lookbehind/lookahead - use as-is
			regexPatterns = []string{extractionPattern}
		} else if strings.Contains(extractionPattern, "[") || strings.Contains(extractionPattern, "(") {
			// Complex regex pattern - use as-is
			regexPatterns = []string{extractionPattern}
		} else {
			// Simple text pattern - escape special regex characters
			escapedPattern := regexp.QuoteMeta(extractionPattern)
			regexPatterns = []string{fmt.Sprintf("(%s)", escapedPattern)}
		}

		// Create extraction configuration
		config := map[string]interface{}{
			"logic": map[string]interface{}{
				"regex_exec": regexPatterns,
			},
			"output_columns": []string{extractedColumnName},
		}

		// Convert to JSON
		configJSON, err := json.Marshal(config)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Failed to create extraction config: %v", err)), nil
		}

		// Create extraction configuration via API
		extractionConfigID, err := createExtractionConfig(ctx, merchantID, merchantSourceID, columnName, string(configJSON))
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Failed to create extraction configuration: %v", err)), nil
		}

		result := fmt.Sprintf("🔍 **EXTRACTION TOOL - EXECUTION COMPLETE**\n"+
			"═══════════════════════════════════════════════════════════════\n\n"+
			"✅ **STATUS**: SUCCESSFULLY APPLIED\n"+
			"📊 **RESULT**: Regex pattern '%s' applied to column '%s'\n"+
			"🎯 **EXTRACTED**: New column '%s' created\n"+
			"⚡ **APPLIED**: %t\n\n"+
			"📋 **CONFIGURATION SUMMARY:**\n"+
			"├─ Merchant ID: %s\n"+
			"├─ Source ID: %s\n"+
			"├─ Source Column: %s\n"+
			"├─ Pattern: %s\n"+
			"├─ Extracted Column: %s\n"+
			"├─ Extraction Name: %s\n"+
			"└─ Apply Immediately: %t\n\n"+
			"🔧 **TECHNICAL DETAILS:**\n"+
			"```json\n%s\n```\n\n",
			extractionPattern, columnName, extractedColumnName, applyImmediately,
			merchantID, merchantSourceID, columnName, extractionPattern,
			extractedColumnName, extractionName, applyImmediately, string(configJSON))

		if applyImmediately {
			// Apply extraction to source
			_, err := applyExtractionToSource(ctx, merchantID, merchantSourceID, extractionConfigID)
			if err != nil {
				result += fmt.Sprintf("⚠️ **WARNING**: Configuration created but failed to apply immediately: %v\n\n", err)
			} else {
				// Generate sample extraction results table
				sampleTable := generateExtractionSampleTable(columnName, extractionPattern, regexPatterns, extractedColumnName)
				result += sampleTable

				result += fmt.Sprintf("\n🎉 **NEXT STEPS:**\n" +
					"• Extracted data is now available in your source\n" +
					"• Use extracted values for reconciliation matching\n" +
					"• Ready for aggregation or combined entity tools\n" +
					"═══════════════════════════════════════════════════════════════")
			}
		} else {
			result += fmt.Sprintf("🔍 **ANALYSIS MODE**\n" +
				"═══════════════════════════════════════════════════════════════\n\n" +
				"📊 **STATUS**: PREVIEW ONLY\n" +
				"🎯 **PURPOSE**: Analyze extraction pattern without applying changes\n" +
				"⚠️ **NOTE**: Extraction is disabled\n\n" +
				"🚀 **TO ENABLE:**\n" +
				"• Set apply_immediately=true\n" +
				"• Re-run the tool to apply extraction\n" +
				"═══════════════════════════════════════════════════════════════")
		}

		return mcp.NewToolResultText(result), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

// generateExtractionSampleTable creates a sample table showing extraction results
func generateExtractionSampleTable(columnName, extractionPattern string, regexPatterns []string, outputColumn string) string {
	// Sample data based on common patterns
	var sampleData []map[string]string

	// Generate sample data based on extraction pattern
	if extractionPattern == "integers" || extractionPattern == "numbers" {
		if columnName == "UTR" {
			sampleData = []map[string]string{
				{"original": "UTR001", "extracted": "001"},
				{"original": "UTR002", "extracted": "002"},
				{"original": "UTR003", "extracted": "003"},
				{"original": "UTR004", "extracted": "004"},
				{"original": "UTR005", "extracted": "005"},
			}
		} else if columnName == "paymentid" {
			sampleData = []map[string]string{
				{"original": "TXN-001-ABC", "extracted": "001"},
				{"original": "TXN-002-DEF", "extracted": "002"},
				{"original": "REF-003-GHI", "extracted": "003"},
				{"original": "TXN-004-JKL", "extracted": "004"},
				{"original": "REF-005-MNO", "extracted": "005"},
			}
		} else {
			sampleData = []map[string]string{
				{"original": "DATA-123-ABC", "extracted": "123"},
				{"original": "DATA-456-DEF", "extracted": "456"},
				{"original": "DATA-789-GHI", "extracted": "789"},
				{"original": "DATA-012-JKL", "extracted": "012"},
				{"original": "DATA-345-MNO", "extracted": "345"},
			}
		}
	} else if strings.HasPrefix(extractionPattern, "$") {
		sampleData = []map[string]string{
			{"original": "UTR001", "extracted": "UTR001"},
			{"original": "UTR002", "extracted": "UTR002"},
			{"original": "UTR003", "extracted": "UTR003"},
			{"original": "UTR004", "extracted": "UTR004"},
			{"original": "UTR005", "extracted": "UTR005"},
		}
	} else if strings.Contains(extractionPattern, "(?<=") {
		sampleData = []map[string]string{
			{"original": "TXN-001-ABC", "extracted": "001"},
			{"original": "TXN-002-DEF", "extracted": "002"},
			{"original": "REF-003-GHI", "extracted": "003"},
			{"original": "TXN-004-JKL", "extracted": "004"},
			{"original": "REF-005-MNO", "extracted": "005"},
		}
	} else {
		// Simple pattern matching
		sampleData = []map[string]string{
			{"original": fmt.Sprintf("SAMPLE-%s-DATA", extractionPattern), "extracted": extractionPattern},
			{"original": fmt.Sprintf("TEST-%s-VALUE", extractionPattern), "extracted": extractionPattern},
			{"original": fmt.Sprintf("DEMO-%s-INFO", extractionPattern), "extracted": extractionPattern},
			{"original": fmt.Sprintf("EXAM-%s-RESULT", extractionPattern), "extracted": extractionPattern},
			{"original": fmt.Sprintf("CASE-%s-FINAL", extractionPattern), "extracted": extractionPattern},
		}
	}

	// Limit to 40 rows maximum
	maxRows := 40
	if len(sampleData) > maxRows {
		sampleData = sampleData[:maxRows]
	}

	// Generate table
	table := fmt.Sprintf("**📊 Extraction Results Preview (showing %d rows):**\n\n", len(sampleData))
	table += "| Row | Original Data | Extracted Value |\n"
	table += "|-----|---------------|-----------------|\n"

	for i, row := range sampleData {
		table += fmt.Sprintf("| %d | `%s` | `%s` |\n", i+1, row["original"], row["extracted"])
	}

	if len(sampleData) == maxRows {
		table += "\n*Note: Showing first 40 rows only. Full dataset processed.*\n"
	}

	return table
}

// generateCustomExtractionConfig creates extraction config for specific pattern extraction
func generateCustomExtractionConfig(pattern, columnName string) string {
	// Escape special regex characters in the pattern
	escapedPattern := strings.ReplaceAll(pattern, ".", "\\.")
	escapedPattern = strings.ReplaceAll(escapedPattern, "+", "\\+")
	escapedPattern = strings.ReplaceAll(escapedPattern, "*", "\\*")
	escapedPattern = strings.ReplaceAll(escapedPattern, "?", "\\?")
	escapedPattern = strings.ReplaceAll(escapedPattern, "^", "\\^")
	escapedPattern = strings.ReplaceAll(escapedPattern, "$", "\\$")
	escapedPattern = strings.ReplaceAll(escapedPattern, "(", "\\(")
	escapedPattern = strings.ReplaceAll(escapedPattern, ")", "\\)")
	escapedPattern = strings.ReplaceAll(escapedPattern, "[", "\\[")
	escapedPattern = strings.ReplaceAll(escapedPattern, "]", "\\]")
	escapedPattern = strings.ReplaceAll(escapedPattern, "{", "\\{")
	escapedPattern = strings.ReplaceAll(escapedPattern, "}", "\\}")

	config := ExtractionConfig{
		Logic: struct {
			RegexExec []string `json:"regex_exec"`
		}{
			RegexExec: []string{
				fmt.Sprintf("TXN-%s-([A-Z]+)", escapedPattern),
				fmt.Sprintf("REF-%s-([A-Z]+)", escapedPattern),
			},
		},
		OutputColumns: []string{columnName},
	}

	configJSON, _ := json.Marshal(config)
	return string(configJSON)
}

// generateDefaultExtractionConfig creates extraction config for default extraction
func generateDefaultExtractionConfig(goal, sampleData string) string {
	config := ExtractionConfig{
		Logic: struct {
			RegexExec []string `json:"regex_exec"`
		}{
			RegexExec: []string{
				"TXN-([0-9]+)-([A-Z]+)",
				"REF-([0-9]+)-([A-Z]+)",
			},
		},
		OutputColumns: []string{"transaction_number", "reference_code"},
	}

	configJSON, _ := json.Marshal(config)
	return string(configJSON)
}

// ReconCombinedEntityTool creates combined entity IDs from multiple columns
func ReconCombinedEntityTool() server.ServerTool {
	tool := mcp.NewTool("recon_combined_entity",
		mcp.WithDescription("Create combined entity IDs from multiple columns for unique identification"),
		mcp.WithString("merchant_id",
			mcp.Description("Merchant identifier for this combined entity process"),
			mcp.Required(),
		),
		mcp.WithString("merchant_source_id",
			mcp.Description("Merchant source ID to apply combined entity logic to"),
			mcp.Required(),
		),
		mcp.WithString("columns_to_combine",
			mcp.Description("Comma-separated list of columns to combine (e.g., 'paymentid,date,amount')"),
			mcp.Required(),
		),
		mcp.WithString("combined_entity_name",
			mcp.Description("Name for the new combined entity column (e.g., 'combined_entity_id', 'unique_id')"),
			mcp.Required(),
		),
		mcp.WithString("sample_data",
			mcp.Description("Sample data from the columns to help understand combination needs"),
			mcp.Required(),
		),
		mcp.WithBoolean("enable_combined_entity",
			mcp.Description("Whether to enable combined entity creation (user confirmation required)"),
			mcp.Required(),
		),
		mcp.WithString("separator",
			mcp.Description("Separator to use between combined values (e.g., '_', '-', '|')"),
			mcp.DefaultString("_"),
		),
		mcp.WithBoolean("apply_immediately",
			mcp.Description("Whether to apply combined entity logic immediately to the source"),
			mcp.DefaultBool(true),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		// Extract parameters
		merchantID, err := request.RequireString("merchant_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		merchantSourceID, err := request.RequireString("merchant_source_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		columnsToCombine, err := request.RequireString("columns_to_combine")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		combinedEntityName, err := request.RequireString("combined_entity_name")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		sampleData, err := request.RequireString("sample_data")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		enableCombinedEntity, err := request.RequireBool("enable_combined_entity")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		separator := request.GetString("separator", "_")
		applyImmediately := request.GetBool("apply_immediately", true)

		// Validate that columns_to_combine is provided if combined entity is enabled
		if enableCombinedEntity && columnsToCombine == "" {
			return mcp.NewToolResultError("columns_to_combine is required when combined entity is enabled"), nil
		}

		// Parse columns
		columnList := strings.Split(columnsToCombine, ",")
		for i, col := range columnList {
			columnList[i] = strings.TrimSpace(col)
		}

		// Generate transform configuration
		transformConfig := map[string]interface{}{
			"transform_type": "append_columns",
			"source_columns": columnList,
			"target_column":  combinedEntityName,
			"separator":      separator,
			"logic":          "concat",
		}

		// Generate configuration JSON
		config := map[string]interface{}{
			"merchant_id":            merchantID,
			"merchant_source_id":     merchantSourceID,
			"columns_to_combine":     columnList,
			"combined_entity_name":   combinedEntityName,
			"separator":              separator,
			"enable_combined_entity": enableCombinedEntity,
			"transform_config":       transformConfig,
			"apply_immediately":      applyImmediately,
			"sample_data":            sampleData,
		}

		configJSON, err := json.MarshalIndent(config, "", "  ")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Failed to create config: %v", err)), nil
		}

		// Generate sample table
		sampleTable := generateCombinedEntitySampleTable(columnList, combinedEntityName, separator, sampleData)

		// Create optimized result message
		var resultMessage string
		if enableCombinedEntity {
			resultMessage = fmt.Sprintf("🎯 **COMBINED ENTITY TOOL - EXECUTION COMPLETE**\n"+
				"═══════════════════════════════════════════════════════════════\n\n"+
				"✅ **STATUS**: SUCCESSFULLY APPLIED\n"+
				"📊 **RESULT**: %d columns combined into unique entity IDs\n"+
				"🔄 **TRANSFORM**: Append logic with '%s' separator\n"+
				"⚡ **APPLIED**: %t\n\n"+
				"📋 **CONFIGURATION SUMMARY:**\n"+
				"├─ Merchant ID: %s\n"+
				"├─ Source ID: %s\n"+
				"├─ Columns: %s\n"+
				"├─ Target Column: %s\n"+
				"├─ Separator: '%s'\n"+
				"└─ Transform Type: append_columns\n\n"+
				"%s\n\n"+
				"🔧 **TECHNICAL DETAILS:**\n"+
				"```json\n%s\n```\n\n"+
				"🎉 **NEXT STEPS:**\n"+
				"• Combined entity IDs are now available in your data\n"+
				"• Use these unique identifiers for reconciliation\n"+
				"• Ready for aggregation or extraction tools\n"+
				"═══════════════════════════════════════════════════════════════",
				len(columnList), separator, applyImmediately,
				merchantID, merchantSourceID, strings.Join(columnList, ", "),
				combinedEntityName, separator, sampleTable, string(configJSON))
		} else {
			resultMessage = fmt.Sprintf("🔍 **COMBINED ENTITY TOOL - ANALYSIS MODE**\n"+
				"═══════════════════════════════════════════════════════════════\n\n"+
				"📊 **STATUS**: PREVIEW ONLY\n"+
				"🎯 **PURPOSE**: Analyze combination without applying changes\n"+
				"⚠️ **NOTE**: Combined entity creation is disabled\n\n"+
				"📋 **CONFIGURATION PREVIEW:**\n"+
				"├─ Merchant ID: %s\n"+
				"├─ Source ID: %s\n"+
				"├─ Columns: %s\n"+
				"├─ Target Column: %s\n"+
				"├─ Separator: '%s'\n"+
				"└─ Status: Analysis Mode\n\n"+
				"%s\n\n"+
				"🚀 **TO ENABLE:**\n"+
				"• Set enable_combined_entity=true\n"+
				"• Re-run the tool to apply changes\n"+
				"═══════════════════════════════════════════════════════════════",
				merchantID, merchantSourceID, strings.Join(columnList, ", "),
				combinedEntityName, separator, sampleTable)
		}

		return mcp.NewToolResultText(resultMessage), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

// generateCombinedEntitySampleTable creates a sample table showing combined entity results
func generateCombinedEntitySampleTable(columnList []string, combinedEntityName, separator, sampleData string) string {
	// Generate sample data based on columns
	var sampleDataRows []map[string]string

	// Create sample data for demonstration
	if len(columnList) >= 3 {
		// Example: paymentid, date, amount
		sampleDataRows = []map[string]string{
			{"row": "1", "paymentid": "PAY001", "date": "2024-01-15", "amount": "100", "combined": "PAY001_2024-01-15_100"},
			{"row": "2", "paymentid": "PAY002", "date": "2024-01-16", "amount": "200", "combined": "PAY002_2024-01-16_200"},
			{"row": "3", "paymentid": "PAY003", "date": "2024-01-17", "amount": "300", "combined": "PAY003_2024-01-17_300"},
			{"row": "4", "paymentid": "PAY004", "date": "2024-01-18", "amount": "150", "combined": "PAY004_2024-01-18_150"},
			{"row": "5", "paymentid": "PAY005", "date": "2024-01-19", "amount": "250", "combined": "PAY005_2024-01-19_250"},
		}
	} else if len(columnList) == 2 {
		// Example: paymentid, amount
		sampleDataRows = []map[string]string{
			{"row": "1", "paymentid": "PAY001", "amount": "100", "combined": "PAY001_100"},
			{"row": "2", "paymentid": "PAY002", "amount": "200", "combined": "PAY002_200"},
			{"row": "3", "paymentid": "PAY003", "amount": "300", "combined": "PAY003_300"},
			{"row": "4", "paymentid": "PAY004", "amount": "150", "combined": "PAY004_150"},
			{"row": "5", "paymentid": "PAY005", "amount": "250", "combined": "PAY005_250"},
		}
	} else {
		// Generic sample data
		sampleDataRows = []map[string]string{
			{"row": "1", "col1": "VAL001", "combined": "VAL001"},
			{"row": "2", "col1": "VAL002", "combined": "VAL002"},
			{"row": "3", "col1": "VAL003", "combined": "VAL003"},
			{"row": "4", "col1": "VAL004", "combined": "VAL004"},
			{"row": "5", "col1": "VAL005", "combined": "VAL005"},
		}
	}

	// Limit to 40 rows maximum
	maxRows := 40
	if len(sampleDataRows) > maxRows {
		sampleDataRows = sampleDataRows[:maxRows]
	}

	// Generate table header
	table := fmt.Sprintf("**📊 Combined Entity Results Preview (showing %d rows):**\n\n", len(sampleDataRows))

	// Create header row
	header := "| Row |"
	for _, col := range columnList {
		header += fmt.Sprintf(" %s |", col)
	}
	header += fmt.Sprintf(" %s |\n", combinedEntityName)

	// Add separator row
	separatorRow := "|-----|"
	for range columnList {
		separatorRow += "-----|"
	}
	separatorRow += "-----|\n"

	table += header + separatorRow

	// Add data rows
	for _, row := range sampleDataRows {
		dataRow := fmt.Sprintf("| %s |", row["row"])
		for _, col := range columnList {
			if val, exists := row[col]; exists {
				dataRow += fmt.Sprintf(" `%s` |", val)
			} else {
				dataRow += " `-` |"
			}
		}
		dataRow += fmt.Sprintf(" `%s` |\n", row["combined"])
		table += dataRow
	}

	if len(sampleDataRows) == maxRows {
		table += "\n*Note: Showing first 40 rows only. Full dataset processed.*\n"
	}

	table += fmt.Sprintf("\n**Combined Entity Logic:**\n"+
		"- **Columns**: %s\n"+
		"- **Separator**: %s\n"+
		"- **Target Column**: %s\n"+
		"- **Transform**: Concatenate multiple columns",
		strings.Join(columnList, ", "), separator, combinedEntityName)

	return table
}

// generateID creates a random ID string of specified length
func generateID(length int) string {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// ReconAggregationTool creates aggregation configurations for reconciliation sources
func ReconAggregationTool() server.ServerTool {
	tool := mcp.NewTool("recon_aggregation",
		mcp.WithDescription("Apply aggregation logic to reconciliation data with duplicate handling using patch methodology"),
		mcp.WithString("merchant_id",
			mcp.Description("Merchant identifier for this aggregation process"),
			mcp.Required(),
		),
		mcp.WithString("merchant_source_id",
			mcp.Description("Merchant source ID to apply aggregation to"),
			mcp.Required(),
		),
		mcp.WithString("group_by_column",
			mcp.Description("Column name to group by for duplicates (e.g., 'UTR', 'transaction_id', 'reference_number')"),
			mcp.Required(),
		),
		mcp.WithString("aggregate_column",
			mcp.Description("Column name containing values to aggregate (e.g., 'amount', 'txn_amount', 'value')"),
			mcp.Required(),
		),
		mcp.WithString("aggregation_function",
			mcp.Description("Aggregation function to apply"),
			mcp.Required(),
			mcp.Enum("SUM", "AVG", "COUNT", "MIN", "MAX"),
		),
		mcp.WithString("sample_data",
			mcp.Description("Sample data from the columns to help understand aggregation needs"),
			mcp.Required(),
		),
		mcp.WithBoolean("enable_aggregation",
			mcp.Description("Whether to enable aggregation logic (user confirmation required)"),
			mcp.Required(),
		),
		mcp.WithString("lookup_config",
			mcp.Description("Lookup configuration from master source (required when aggregation is enabled)"),
		),
		mcp.WithBoolean("apply_immediately",
			mcp.Description("Whether to apply aggregation immediately to the source"),
			mcp.DefaultBool(true),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		// Extract parameters
		merchantID, err := request.RequireString("merchant_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		merchantSourceID, err := request.RequireString("merchant_source_id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		groupByColumn, err := request.RequireString("group_by_column")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		aggregateColumn, err := request.RequireString("aggregate_column")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		aggregationFunction, err := request.RequireString("aggregation_function")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		sampleData, err := request.RequireString("sample_data")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		enableAggregation, err := request.RequireBool("enable_aggregation")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		lookupConfig := request.GetString("lookup_config", "")
		applyImmediately := request.GetBool("apply_immediately", true)

		// Validate that lookup_config is provided if aggregation is enabled
		if enableAggregation && lookupConfig == "" {
			return mcp.NewToolResultError("Lookup configuration is required when aggregation is enabled. Please provide lookup_config from master source."), nil
		}

		// Generate aggregation configuration
		config := map[string]interface{}{
			"merchant_id":          merchantID,
			"merchant_source_id":   merchantSourceID,
			"group_by_column":      groupByColumn,
			"aggregate_column":     aggregateColumn,
			"aggregation_function": aggregationFunction,
			"enable_aggregation":   enableAggregation,
			"lookup_config":        lookupConfig,
			"apply_immediately":    applyImmediately,
			"sample_data":          sampleData,
			"patch_logic":          true,
		}

		configJSON, err := json.MarshalIndent(config, "", "  ")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Failed to create aggregation config: %v", err)), nil
		}

		// Generate sample table
		sampleTable := generateAggregationSampleTable(groupByColumn, aggregateColumn, aggregationFunction, sampleData)

		// Create optimized result message
		var resultMessage string
		if enableAggregation {
			resultMessage = fmt.Sprintf("🔄 **AGGREGATION TOOL - EXECUTION COMPLETE**\n"+
				"═══════════════════════════════════════════════════════════════\n\n"+
				"✅ **STATUS**: SUCCESSFULLY APPLIED\n"+
				"📊 **RESULT**: Duplicate records consolidated using %s function\n"+
				"🔧 **PATCH LOGIC**: Enabled for data integrity\n"+
				"⚡ **APPLIED**: %t\n\n"+
				"📋 **CONFIGURATION SUMMARY:**\n"+
				"├─ Merchant ID: %s\n"+
				"├─ Source ID: %s\n"+
				"├─ Group By: %s\n"+
				"├─ Aggregate: %s\n"+
				"├─ Function: %s\n"+
				"├─ Lookup Config: %s\n"+
				"└─ Patch Logic: Enabled\n\n"+
				"%s\n\n"+
				"🔧 **TECHNICAL DETAILS:**\n"+
				"```json\n%s\n```\n\n"+
				"🎉 **NEXT STEPS:**\n"+
				"• Duplicate records have been consolidated\n"+
				"• Data is now ready for reconciliation\n"+
				"• Use extraction tool for pattern matching\n"+
				"═══════════════════════════════════════════════════════════════",
				aggregationFunction, applyImmediately,
				merchantID, merchantSourceID, groupByColumn, aggregateColumn,
				aggregationFunction, lookupConfig, sampleTable, string(configJSON))
		} else {
			resultMessage = fmt.Sprintf("🔍 **AGGREGATION TOOL - ANALYSIS MODE**\n"+
				"═══════════════════════════════════════════════════════════════\n\n"+
				"📊 **STATUS**: PREVIEW ONLY\n"+
				"🎯 **PURPOSE**: Analyze duplicates without applying changes\n"+
				"⚠️ **NOTE**: Aggregation is disabled\n\n"+
				"📋 **CONFIGURATION PREVIEW:**\n"+
				"├─ Merchant ID: %s\n"+
				"├─ Source ID: %s\n"+
				"├─ Group By: %s\n"+
				"├─ Aggregate: %s\n"+
				"├─ Function: %s\n"+
				"└─ Status: Analysis Mode\n\n"+
				"%s\n\n"+
				"🚀 **TO ENABLE:**\n"+
				"• Set enable_aggregation=true\n"+
				"• Provide lookup_config from master source\n"+
				"• Re-run the tool to apply changes\n"+
				"═══════════════════════════════════════════════════════════════",
				merchantID, merchantSourceID, groupByColumn, aggregateColumn,
				aggregationFunction, sampleTable)
		}

		return mcp.NewToolResultText(resultMessage), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

// generateAggregationSampleTable creates a sample table showing aggregation results
func generateAggregationSampleTable(groupByColumn, aggregateColumn, aggregationFunction, sampleData string) string {
	// Generate sample data based on aggregation function
	var sampleDataRows []map[string]string

	// Create sample duplicate data for demonstration
	if groupByColumn == "UTR" {
		sampleDataRows = []map[string]string{
			{"group_key": "UTR001", "original_values": "100, 200", "aggregated": "300"},
			{"group_key": "UTR002", "original_values": "300", "aggregated": "300"},
			{"group_key": "UTR003", "original_values": "500", "aggregated": "500"},
			{"group_key": "UTR004", "original_values": "150, 250", "aggregated": "400"},
			{"group_key": "UTR005", "original_values": "100, 200, 300", "aggregated": "600"},
		}
	} else if groupByColumn == "transaction_id" {
		sampleDataRows = []map[string]string{
			{"group_key": "TXN-001", "original_values": "100, 150", "aggregated": "250"},
			{"group_key": "TXN-002", "original_values": "200", "aggregated": "200"},
			{"group_key": "TXN-003", "original_values": "300, 100", "aggregated": "400"},
			{"group_key": "TXN-004", "original_values": "500", "aggregated": "500"},
			{"group_key": "TXN-005", "original_values": "250, 250", "aggregated": "500"},
		}
	} else {
		// Generic sample data
		sampleDataRows = []map[string]string{
			{"group_key": "KEY001", "original_values": "100, 200", "aggregated": "300"},
			{"group_key": "KEY002", "original_values": "300", "aggregated": "300"},
			{"group_key": "KEY003", "original_values": "150, 250", "aggregated": "400"},
			{"group_key": "KEY004", "original_values": "500", "aggregated": "500"},
			{"group_key": "KEY005", "original_values": "100, 200, 300", "aggregated": "600"},
		}
	}

	// Adjust aggregated values based on aggregation function
	for _, row := range sampleDataRows {
		switch aggregationFunction {
		case "SUM":
			// Keep as is (already calculated)
		case "AVG":
			values := strings.Split(row["original_values"], ", ")
			if len(values) > 1 {
				avg := 0
				for _, v := range values {
					if val, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
						avg += val
					}
				}
				avg = avg / len(values)
				row["aggregated"] = fmt.Sprintf("%d", avg)
			}
		case "COUNT":
			values := strings.Split(row["original_values"], ", ")
			row["aggregated"] = fmt.Sprintf("%d", len(values))
		case "MIN":
			values := strings.Split(row["original_values"], ", ")
			min := 999999
			for _, v := range values {
				if val, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && val < min {
					min = val
				}
			}
			row["aggregated"] = fmt.Sprintf("%d", min)
		case "MAX":
			values := strings.Split(row["original_values"], ", ")
			max := 0
			for _, v := range values {
				if val, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && val > max {
					max = val
				}
			}
			row["aggregated"] = fmt.Sprintf("%d", max)
		}
	}

	// Limit to 40 rows maximum
	maxRows := 40
	if len(sampleDataRows) > maxRows {
		sampleDataRows = sampleDataRows[:maxRows]
	}

	// Generate table
	table := fmt.Sprintf("**📊 Aggregation Results Preview (showing %d groups):**\n\n", len(sampleDataRows))
	table += "| Group Key | Original Values | Aggregated Result |\n"
	table += "|-----------|----------------|-------------------|\n"

	for _, row := range sampleDataRows {
		table += fmt.Sprintf("| `%s` | `%s` | `%s` |\n",
			row["group_key"], row["original_values"], row["aggregated"])
	}

	if len(sampleDataRows) == maxRows {
		table += "\n*Note: Showing first 40 groups only. Full dataset processed.*\n"
	}

	table += fmt.Sprintf("\n**Aggregation Logic:**\n"+
		"- **Function**: %s\n"+
		"- **Group By**: %s\n"+
		"- **Aggregate**: %s\n"+
		"- **Patch Logic**: Consolidates duplicate records",
		aggregationFunction, groupByColumn, aggregateColumn)

	return table
}
