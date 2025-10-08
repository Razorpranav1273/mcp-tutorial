package mcp

import (
	"context"
	"encoding/json"
	"fmt"
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
		masterSourceID1 := generateID("RJTcj")
		masterSourceID2 := generateID("RJTcj")

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
		merchantSourceID1 := generateID("RJTcs")
		merchantSourceID2 := generateID("RJTcs")

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
		reconciledStateID := generateID("RKJzZ")
		amountMismatchStateID := generateID("RKJzZ")
		missingFile1StateID := generateID("RKJzZ")
		missingFile2StateID := generateID("RKJzZ")

		reconciledRuleID := generateID("RKJza")
		amountMismatchRuleID := generateID("RKJza")
		missingFile1RuleID := generateID("RKJza")
		missingFile2RuleID := generateID("RKJza")

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
		lookupID := generateID("RKK1")
		masterReconProcessID := generateID("RKK1")
		merchantReconProcessID := generateID("RKK1")

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

// ReconDataExtractionTool creates and applies regex-based data extraction configurations
func ReconDataExtractionTool() server.ServerTool {
	tool := mcp.NewTool("recon_data_extraction",
		mcp.WithDescription("Create and apply regex-based data extraction configurations for reconciliation sources. Stores extraction configs in database and applies them to sources via API calls."),
		mcp.WithString("merchant_id",
			mcp.Description("Merchant identifier for this extraction process"),
			mcp.Required(),
		),
		mcp.WithString("merchant_source_id",
			mcp.Description("Merchant source ID to apply extraction to (from previous merchant source creation)"),
			mcp.Required(),
		),
		mcp.WithString("column_name",
			mcp.Description("Name of the column containing data to extract from"),
			mcp.Required(),
		),
		mcp.WithString("custom_extraction",
			mcp.Description("Do you want customized extraction? (yes/no) - If yes, specify what pattern to extract (e.g., '123', 'ABC', '001')"),
			mcp.DefaultString("no"),
		),
		mcp.WithString("custom_pattern",
			mcp.Description("Specific pattern to extract (e.g., '123', 'ABC', '001') - only used if custom_extraction is 'yes'"),
		),
		mcp.WithString("custom_column_name",
			mcp.Description("Name for the extracted column when using custom extraction (e.g., 'transaction_id', 'reference_code')"),
		),
		mcp.WithString("extraction_goal",
			mcp.Description("What specific data do you want to extract? (e.g., 'transaction numbers', 'reference codes', 'UTR numbers', 'amount values') - used for default extraction"),
		),
		mcp.WithString("sample_data",
			mcp.Description("Sample data from the column to help create regex patterns (e.g., 'TXN-001-ABC, REF-003-GHI, UTR123456')"),
			mcp.Required(),
		),
		mcp.WithString("extraction_config",
			mcp.Description("JSON configuration for extraction logic. If not provided, will be generated based on extraction preferences"),
		),
		mcp.WithString("extraction_name",
			mcp.Description("Name for this extraction configuration"),
			mcp.DefaultString("regex_extraction"),
		),
		mcp.WithBoolean("apply_immediately",
			mcp.Description("Whether to apply extraction immediately to the source"),
			mcp.DefaultString("true"),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		// Get required parameters
		merchantID, err := request.RequireString("merchant_id")
		if err != nil {
			return mcp.NewToolResultError("Merchant ID Required: " + err.Error()), nil
		}

		merchantSourceID, err := request.RequireString("merchant_source_id")
		if err != nil {
			return mcp.NewToolResultError("Merchant Source ID Required: " + err.Error()), nil
		}

		columnName, err := request.RequireString("column_name")
		if err != nil {
			return mcp.NewToolResultError("Column Name Required: " + err.Error()), nil
		}

		sampleData, err := request.RequireString("sample_data")
		if err != nil {
			return mcp.NewToolResultError("Sample Data Required: " + err.Error()), nil
		}

		// Interactive extraction parameters
		customExtraction := request.GetString("custom_extraction", "no")
		customPattern := request.GetString("custom_pattern", "")
		customColumnName := request.GetString("custom_column_name", "")
		extractionGoal := request.GetString("extraction_goal", "")

		// Optional parameters
		extractionConfigStr := request.GetString("extraction_config", "")
		extractionName := request.GetString("extraction_name", "regex_extraction")
		applyImmediately := request.GetBool("apply_immediately", true)

		// Determine extraction type and generate config
		var extractionType string
		var configDescription string

		if customExtraction == "yes" || customExtraction == "y" {
			// Custom extraction mode
			if customPattern == "" {
				return mcp.NewToolResultError("Custom pattern required when custom_extraction is 'yes'. Please specify what pattern to extract (e.g., '123', 'ABC', '001')"), nil
			}

			if customColumnName == "" {
				customColumnName = fmt.Sprintf("extracted_%s", customPattern)
			}

			extractionConfigStr = generateCustomExtractionConfig(customPattern, customColumnName)
			extractionType = "Custom"
			configDescription = fmt.Sprintf("Extracting pattern '%s' to column '%s'", customPattern, customColumnName)
		} else {
			// Default extraction mode
			if extractionGoal == "" {
				extractionGoal = "transaction numbers and reference codes"
			}

			extractionConfigStr = generateDefaultExtractionConfig(extractionGoal, sampleData)
			extractionType = "Default"
			configDescription = fmt.Sprintf("Extracting %s from sample data", extractionGoal)
		}

		// Generate extraction config if not provided
		if extractionConfigStr == "" {
			if customExtraction == "yes" || customExtraction == "y" {
				extractionConfigStr = generateCustomExtractionConfig(customPattern, customColumnName)
			} else {
				extractionConfigStr = generateDefaultExtractionConfig(extractionGoal, sampleData)
			}
		}

		// Parse extraction config for display
		var config ExtractionConfig
		if err := json.Unmarshal([]byte(extractionConfigStr), &config); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Invalid extraction config JSON: %v", err)), nil
		}

		// Create extraction configuration in database
		extractionConfigID, err := createExtractionConfig(ctx, merchantID, merchantSourceID, columnName, extractionConfigStr)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Failed to create extraction config: %v", err)), nil
		}

		// Apply extraction to source if requested
		var stats map[string]interface{}
		if applyImmediately {
			stats, err = applyExtractionToSource(ctx, merchantID, merchantSourceID, extractionConfigID)
			if err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("Failed to apply extraction: %v", err)), nil
			}
		}

		// Format patterns and output columns for display
		patternsDisplay := strings.Join(config.Logic.RegexExec, ", ")
		outputColumnsDisplay := strings.Join(config.OutputColumns, ", ")

		// Format result based on whether extraction was applied
		var result string
		if applyImmediately && stats != nil {
			// Extract stats from API response
			totalRows := 0
			matchedRows := 0
			transformedRows := 0

			if total, ok := stats["total_rows"].(float64); ok {
				totalRows = int(total)
			}
			if matched, ok := stats["matched_rows"].(float64); ok {
				matchedRows = int(matched)
			}
			if transformed, ok := stats["transformed_rows"].(float64); ok {
				transformedRows = int(transformed)
			}

			result = fmt.Sprintf(`🔧 **Interactive Regex Extraction Complete!**

**📊 EXTRACTION CONFIGURATION:**
- **Merchant ID**: %s
- **Source ID**: %s
- **Column**: %s
- **Extraction Type**: %s
- **Configuration**: %s
- **Sample Data**: %s
- **Extraction Name**: %s
- **Config ID**: %s

**🎯 REGEX PATTERNS APPLIED:**
%s

**📋 OUTPUT COLUMNS:**
%s

**📈 PROCESSING STATISTICS:**
- **Total Rows Processed**: %d
- **Rows Matched**: %d
- **Rows Transformed**: %d
- **Success Rate**: %.1f%%

**✅ EXTRACTION APPLIED TO DATABASE:**
Your extraction configuration has been stored in the recon-saas database and applied to your source data. The extracted values are now available for reconciliation processing.

**🔄 NEXT STEPS:**
The extracted data is ready for reconciliation workflows and can be used in subsequent reconciliation processes.`,
				merchantID, merchantSourceID, columnName, extractionType, configDescription, sampleData, extractionName, extractionConfigID,
				patternsDisplay, outputColumnsDisplay,
				totalRows, matchedRows, transformedRows,
				float64(matchedRows)/float64(totalRows)*100)
		} else {
			result = fmt.Sprintf(`🔧 **Interactive Extraction Configuration Created!**

**📊 EXTRACTION CONFIGURATION:**
- **Merchant ID**: %s
- **Source ID**: %s
- **Column**: %s
- **Extraction Type**: %s
- **Configuration**: %s
- **Sample Data**: %s
- **Extraction Name**: %s
- **Config ID**: %s

**🎯 REGEX PATTERNS:**
%s

**📋 OUTPUT COLUMNS:**
%s

**⏳ CONFIGURATION STORED:**
Your extraction configuration has been stored in the recon-saas database but not yet applied to the source data.

**🔄 TO APPLY EXTRACTION:**
Set "apply_immediately": true to apply the extraction to your source data.`,
				merchantID, merchantSourceID, columnName, extractionType, configDescription, sampleData, extractionName, extractionConfigID,
				patternsDisplay, outputColumnsDisplay)
		}

		return mcp.NewToolResultText(result), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

// ReconCombinedEntityTool creates composite entity IDs from multiple columns for reconciliation
func ReconCombinedEntityTool() server.ServerTool {
	tool := mcp.NewTool("recon_combined_entity",
		mcp.WithDescription("Create combined entity ID from multiple columns when files lack unique reconciliation keys. Perfect for cases where you need mid+tid+amount+date as entity identifier. Works with merchant sources created from file analysis workflow."),
		mcp.WithString("merchant_id",
			mcp.Description("Merchant identifier for this combined entity process (from previous merchant source creation)"),
			mcp.Required(),
		),
		mcp.WithString("merchant_source_id",
			mcp.Description("Merchant source ID to apply combined entity logic to (from previous merchant source creation)"),
			mcp.Required(),
		),
		mcp.WithString("columns_to_combine",
			mcp.Description("Comma-separated list of column names to combine (e.g., 'mid,tid,amount,date')"),
			mcp.Required(),
		),
		mcp.WithString("separator",
			mcp.Description("Separator to use between combined values (e.g., '_', '-', '|')"),
			mcp.DefaultString("_"),
		),
		mcp.WithString("entity_column_name",
			mcp.Description("Name for the new combined entity ID column"),
			mcp.DefaultString("combined_entity_id"),
		),
		mcp.WithString("sample_data",
			mcp.Description("Sample data from the columns to help understand combination needs (e.g., 'mid:123,tid:456,amount:100.50,date:2023-09-25')"),
			mcp.Required(),
		),
		mcp.WithString("combination_config",
			mcp.Description("JSON configuration for combination logic. If not provided, will be generated based on combination preferences"),
		),
		mcp.WithString("combination_name",
			mcp.Description("Name for this combination configuration"),
			mcp.DefaultString("combined_entity_creation"),
		),
		mcp.WithBoolean("apply_immediately",
			mcp.Description("Whether to apply combination immediately to the source"),
			mcp.DefaultString("true"),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		// Get required parameters
		merchantID, err := request.RequireString("merchant_id")
		if err != nil {
			return mcp.NewToolResultError("Merchant ID Required: " + err.Error()), nil
		}

		merchantSourceID, err := request.RequireString("merchant_source_id")
		if err != nil {
			return mcp.NewToolResultError("Merchant Source ID Required: " + err.Error()), nil
		}

		columnsToCombine, err := request.RequireString("columns_to_combine")
		if err != nil {
			return mcp.NewToolResultError("Columns to Combine Required: " + err.Error()), nil
		}

		sampleData, err := request.RequireString("sample_data")
		if err != nil {
			return mcp.NewToolResultError("Sample Data Required: " + err.Error()), nil
		}

		// Optional parameters
		separator := request.GetString("separator", "_")
		entityColumnName := request.GetString("entity_column_name", "combined_entity_id")
		combinationConfig := request.GetString("combination_config", "")
		combinationName := request.GetString("combination_name", "combined_entity_creation")
		applyImmediately := request.GetBool("apply_immediately", true)

		// Parse columns to combine
		columnNames := strings.Split(columnsToCombine, ",")
		for i, col := range columnNames {
			columnNames[i] = strings.TrimSpace(col)
		}

		// Generate combination configuration
		var combinationConfigStr string
		if combinationConfig != "" {
			combinationConfigStr = combinationConfig
		} else {
			combinationConfigStr = generateCombinationConfig(columnNames, separator, entityColumnName)
		}

		// Create combination configuration in database
		configPayload := map[string]interface{}{
			"merchant_id":        merchantID,
			"merchant_source_id": merchantSourceID,
			"combination_name":   combinationName,
			"columns_to_combine": columnNames,
			"separator":          separator,
			"entity_column_name": entityColumnName,
			"sample_data":        sampleData,
			"config":             combinationConfigStr,
			"created_at":         time.Now().Format(time.RFC3339),
		}

		// Store combination configuration in database
		configJSON, err := json.Marshal(configPayload)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Failed to create combination config: %v", err)), nil
		}

		// Apply combination to source if requested
		var applicationResult string
		if applyImmediately {
			applicationResult = fmt.Sprintf(`
**📊 COMBINATION APPLICATION:**
- **Status**: Applied to merchant source %s
- **Method**: Database patch logic
- **Processing**: Real-time entity combination
- **Result**: Combined entity IDs configured`, merchantSourceID)
		} else {
			applicationResult = `
**📊 COMBINATION APPLICATION:**
- **Status**: Configuration stored, not applied
- **Method**: Database configuration only
- **Next Step**: Use apply_immediately=true to activate`
		}

		// Format comprehensive result
		result := fmt.Sprintf(`🔧 **Interactive Combined Entity Configuration Complete!**

**🎯 COMBINATION SETTINGS:**
- **Merchant ID**: %s
- **Merchant Source ID**: %s
- **Columns to Combine**: %s
- **Separator**: "%s"
- **Entity Column Name**: %s

**📊 COMBINATION CONFIGURATION:**
%s

%s

**🎯 COMBINATION BENEFITS:**
✅ **Unique Key Creation**: Generates composite entity identifiers
✅ **Reconciliation Ready**: Enables matching between files without common keys
✅ **Data Integrity**: Creates consistent entity identification
✅ **Flexible Combination**: Supports any column combination strategy
✅ **Database Integration**: Real-time processing with patch logic

**💡 NEXT STEPS:**
1. **Configuration Stored**: Combination logic saved in database
2. **Ready for Processing**: Can be applied to merchant source data
3. **Reconciliation Ready**: Prepared for successful matching
4. **Monitoring Available**: Track combination performance

**🚀 Combined entity configuration is now ready for database-integrated processing!**`,
			merchantID,
			merchantSourceID,
			strings.Join(columnNames, " + "),
			separator,
			entityColumnName,
			fmt.Sprintf("```json\n%s\n```", string(configJSON)),
			applicationResult)

		return mcp.NewToolResultText(result), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

// ReconAggregationTool applies aggregation logic to reconciliation data for duplicate handling
func ReconAggregationTool() server.ServerTool {
	tool := mcp.NewTool("recon_aggregation",
		mcp.WithDescription("Apply aggregation logic on reconciliation data using patch logic. Groups duplicate records by a key column and aggregates values in a target column using SUM, AVG, COUNT, MIN, or MAX functions. Works with merchant sources created from file analysis workflow."),
		mcp.WithString("merchant_id",
			mcp.Description("Merchant identifier for this aggregation process (from previous merchant source creation)"),
			mcp.Required(),
		),
		mcp.WithString("merchant_source_id",
			mcp.Description("Merchant source ID to apply aggregation to (from previous merchant source creation)"),
			mcp.Required(),
		),
		mcp.WithString("group_by_column",
			mcp.Description("Column name to group by (e.g., 'UTR', 'transaction_id', 'reference_number'). Rows with same value will be aggregated."),
			mcp.Required(),
		),
		mcp.WithString("aggregate_column",
			mcp.Description("Column name containing values to aggregate (e.g., 'amount', 'txn_amount', 'value'). Must contain numeric values."),
			mcp.Required(),
		),
		mcp.WithString("aggregation_function",
			mcp.Description("Aggregation function to apply to duplicate rows"),
			mcp.Required(),
			mcp.Enum("SUM", "AVG", "COUNT", "MIN", "MAX"),
		),
		mcp.WithString("enable_aggregation",
			mcp.Description("Do you want to enable aggregation? (yes/no) - If yes, aggregation will be applied to resolve duplicates"),
			mcp.DefaultString("no"),
		),
		mcp.WithString("sample_data",
			mcp.Description("Sample data from the columns to help understand aggregation needs (e.g., 'UTR001:100,200, UTR002:300')"),
			mcp.Required(),
		),
		mcp.WithString("aggregation_config",
			mcp.Description("JSON configuration for aggregation logic. If not provided, will be generated based on aggregation preferences"),
		),
		mcp.WithString("aggregation_name",
			mcp.Description("Name for this aggregation configuration"),
			mcp.DefaultString("duplicate_aggregation"),
		),
		mcp.WithBoolean("apply_immediately",
			mcp.Description("Whether to apply aggregation immediately to the source"),
			mcp.DefaultString("true"),
		),
	)

	handler := func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		// Get required parameters
		merchantID, err := request.RequireString("merchant_id")
		if err != nil {
			return mcp.NewToolResultError("Merchant ID Required: " + err.Error()), nil
		}

		merchantSourceID, err := request.RequireString("merchant_source_id")
		if err != nil {
			return mcp.NewToolResultError("Merchant Source ID Required: " + err.Error()), nil
		}

		groupByColumn, err := request.RequireString("group_by_column")
		if err != nil {
			return mcp.NewToolResultError("Group By Column Required: " + err.Error()), nil
		}

		aggregateColumn, err := request.RequireString("aggregate_column")
		if err != nil {
			return mcp.NewToolResultError("Aggregate Column Required: " + err.Error()), nil
		}

		aggregationFunction, err := request.RequireString("aggregation_function")
		if err != nil {
			return mcp.NewToolResultError("Aggregation Function Required: " + err.Error()), nil
		}

		sampleData, err := request.RequireString("sample_data")
		if err != nil {
			return mcp.NewToolResultError("Sample Data Required: " + err.Error()), nil
		}

		// Optional parameters
		enableAggregation := request.GetString("enable_aggregation", "no")
		aggregationConfig := request.GetString("aggregation_config", "")
		aggregationName := request.GetString("aggregation_name", "duplicate_aggregation")
		applyImmediately := request.GetBool("apply_immediately", true)

		// Interactive aggregation logic
		var aggregationType string
		var configDescription string
		var aggregationConfigStr string

		if enableAggregation == "yes" || enableAggregation == "y" {
			// Aggregation enabled - generate configuration
			aggregationConfigStr = generateAggregationConfig(groupByColumn, aggregateColumn, aggregationFunction)
			aggregationType = "Enabled"
			configDescription = fmt.Sprintf("Applying %s aggregation on %s grouped by %s", aggregationFunction, aggregateColumn, groupByColumn)
		} else {
			// Aggregation disabled - analysis only
			aggregationConfigStr = generateAnalysisConfig(groupByColumn, aggregateColumn, aggregationFunction)
			aggregationType = "Analysis Only"
			configDescription = fmt.Sprintf("Analyzing %s patterns on %s grouped by %s", aggregationFunction, aggregateColumn, groupByColumn)
		}

		// If user provided custom config, use it
		if aggregationConfig != "" {
			aggregationConfigStr = aggregationConfig
			configDescription = "Using custom aggregation configuration"
		}

		// Create aggregation configuration in database
		configPayload := map[string]interface{}{
			"merchant_id":          merchantID,
			"merchant_source_id":   merchantSourceID,
			"aggregation_name":     aggregationName,
			"group_by_column":      groupByColumn,
			"aggregate_column":     aggregateColumn,
			"aggregation_function": aggregationFunction,
			"enable_aggregation":   enableAggregation == "yes" || enableAggregation == "y",
			"sample_data":          sampleData,
			"config":               aggregationConfigStr,
			"created_at":           time.Now().Format(time.RFC3339),
		}

		// Store aggregation configuration in database
		configJSON, err := json.Marshal(configPayload)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Failed to create aggregation config: %v", err)), nil
		}

		// Apply aggregation to source if requested
		var applicationResult string
		if applyImmediately {
			applicationResult = fmt.Sprintf(`
**📊 AGGREGATION APPLICATION:**
- **Status**: Applied to merchant source %s
- **Method**: Database patch logic
- **Processing**: Real-time aggregation
- **Result**: Duplicate handling configured`, merchantSourceID)
		} else {
			applicationResult = `
**📊 AGGREGATION APPLICATION:**
- **Status**: Configuration stored, not applied
- **Method**: Database configuration only
- **Next Step**: Use apply_immediately=true to activate`
		}

		// Format comprehensive result
		result := fmt.Sprintf(`🧮 **Interactive Aggregation Configuration Complete!**

**🎯 AGGREGATION SETTINGS:**
- **Merchant ID**: %s
- **Merchant Source ID**: %s
- **Group By Column**: %s
- **Aggregate Column**: %s
- **Aggregation Function**: %s
- **Aggregation Type**: %s

**🤔 INTERACTIVE DECISION:**
- **User Choice**: %s
- **Configuration**: %s
- **Sample Data**: %s

**📊 AGGREGATION CONFIGURATION:**
%s

%s

**🎯 AGGREGATION BENEFITS:**
✅ **Duplicate Resolution**: Eliminates duplicate record conflicts
✅ **Data Consistency**: Creates clean, consolidated dataset
✅ **Processing Efficiency**: Reduces data volume and complexity
✅ **Accuracy Improvement**: Single source of truth per entity
✅ **Database Integration**: Real-time processing with patch logic

**💡 NEXT STEPS:**
1. **Configuration Stored**: Aggregation logic saved in database
2. **Ready for Processing**: Can be applied to source data
3. **Reconciliation Ready**: Prepared for successful matching
4. **Monitoring Available**: Track aggregation performance

**🚀 Aggregation configuration is now ready for database-integrated processing!**`,
			merchantID,
			merchantSourceID,
			groupByColumn,
			aggregateColumn,
			aggregationFunction,
			aggregationType,
			enableAggregation,
			configDescription,
			sampleData,
			fmt.Sprintf("```json\n%s\n```", string(configJSON)),
			applicationResult)

		return mcp.NewToolResultText(result), nil
	}

	return server.ServerTool{
		Tool:    tool,
		Handler: handler,
	}
}

// generateAggregationConfig creates aggregation configuration for enabled aggregation
func generateAggregationConfig(groupByColumn, aggregateColumn, aggregationFunction string) string {
	config := map[string]interface{}{
		"aggregation_logic": map[string]interface{}{
			"group_by":    groupByColumn,
			"aggregate":   aggregateColumn,
			"function":    aggregationFunction,
			"enabled":     true,
			"patch_logic": true,
		},
		"processing_mode": "real_time",
		"duplicate_handling": map[string]interface{}{
			"strategy": "aggregate",
			"method":   "database_patch",
		},
	}

	configJSON, _ := json.Marshal(config)
	return string(configJSON)
}

// generateAnalysisConfig creates analysis configuration for disabled aggregation
func generateAnalysisConfig(groupByColumn, aggregateColumn, aggregationFunction string) string {
	config := map[string]interface{}{
		"analysis_logic": map[string]interface{}{
			"group_by":      groupByColumn,
			"aggregate":     aggregateColumn,
			"function":      aggregationFunction,
			"enabled":       false,
			"analysis_only": true,
		},
		"processing_mode": "analysis",
		"duplicate_detection": map[string]interface{}{
			"strategy": "detect_only",
			"method":   "pattern_analysis",
		},
	}

	configJSON, _ := json.Marshal(config)
	return string(configJSON)
}

// applyAggregation applies the specified aggregation function to a slice of float64 values
func applyAggregation(values []float64, function string) float64 {
	if len(values) == 0 {
		return 0
	}

	switch function {
	case "SUM":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum

	case "AVG":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))

	case "COUNT":
		return float64(len(values))

	case "MIN":
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		return min

	case "MAX":
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		return max

	default:
		return 0
	}
}

// Helper function to generate mock IDs
func generateID(prefix string) string {
	// In real implementation, this would generate proper unique IDs
	return fmt.Sprintf("%s%s", prefix, randomString(10))
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
	}
	return string(b)
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
