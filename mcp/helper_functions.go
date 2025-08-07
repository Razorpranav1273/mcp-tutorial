package mcp

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Helper functions for recon-saas API interactions

// analyzeCSVFile analyzes a CSV file and returns metadata about columns and data
func analyzeCSVFile(filePath, fileID string) (map[string]interface{}, error) {
	// Open and read the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV file %s: %v", filePath, err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("CSV file %s is empty", filePath)
	}

	// Extract headers and data
	headers := records[0]
	dataRows := records[1:]
	totalRows := len(dataRows)
	totalColumns := len(headers)

	// Analyze each column for patterns and characteristics
	columnAnalysis := make(map[string]map[string]interface{})
	for i, columnName := range headers {
		analysis := analyzeColumn(columnName, i, dataRows)
		columnAnalysis[columnName] = analysis
	}

	// Identify EntityID candidates
	entityIDCandidates := identifyEntityIDCandidates(headers, columnAnalysis)

	// Identify Amount candidates
	amountCandidates := identifyAmountCandidates(headers, columnAnalysis)

	// Determine recommendations
	var recommendedEntityID, recommendedAmount string
	if len(entityIDCandidates) > 0 {
		if entityMap, ok := entityIDCandidates[0].(map[string]interface{}); ok {
			if name, ok := entityMap["column_name"].(string); ok {
				recommendedEntityID = name
			}
		}
	}
	if len(amountCandidates) > 0 {
		if amountMap, ok := amountCandidates[0].(map[string]interface{}); ok {
			if name, ok := amountMap["column_name"].(string); ok {
				recommendedAmount = name
			}
		}
	}

	return map[string]interface{}{
		"filename":             filePath,
		"file_type":            "csv",
		"total_rows":           totalRows,
		"total_columns":        totalColumns,
		"all_columns":          headers,
		"entityid_candidates":  entityIDCandidates,
		"recommended_entityid": recommendedEntityID,
		"amount_candidates":    amountCandidates,
		"recommended_amount":   recommendedAmount,
		"user_selections": map[string]interface{}{
			"selected_entityid": recommendedEntityID,
			"selected_amount":   recommendedAmount,
		},
	}, nil
}

// analyzeColumn analyzes individual column characteristics
func analyzeColumn(columnName string, columnIndex int, dataRows [][]string) map[string]interface{} {
	if len(dataRows) == 0 {
		return map[string]interface{}{
			"unique_count":      0,
			"unique_percentage": 0.0,
			"data_type":         "unknown",
			"sample_values":     []string{},
			"is_numeric":        false,
			"is_monetary":       false,
		}
	}

	// Extract column values
	values := make([]string, 0, len(dataRows))
	uniqueValues := make(map[string]bool)
	numericCount := 0
	monetaryCount := 0

	sampleSize := len(dataRows)
	if sampleSize > 100 {
		sampleSize = 100 // Analyze first 100 rows for performance
	}

	for i := 0; i < sampleSize && i < len(dataRows); i++ {
		if columnIndex < len(dataRows[i]) {
			value := strings.TrimSpace(dataRows[i][columnIndex])
			if value != "" {
				values = append(values, value)
				uniqueValues[value] = true

				// Check if numeric
				if isNumeric(value) {
					numericCount++
				}

				// Check if monetary
				if isMonetary(value) {
					monetaryCount++
				}
			}
		}
	}

	uniqueCount := len(uniqueValues)
	uniquePercentage := float64(uniqueCount) / float64(len(values)) * 100

	// Get sample values (first 5 unique values)
	sampleValues := make([]string, 0, 5)
	count := 0
	for value := range uniqueValues {
		if count >= 5 {
			break
		}
		sampleValues = append(sampleValues, value)
		count++
	}

	// Determine data type
	dataType := "string"
	isNumericCol := float64(numericCount)/float64(len(values)) > 0.8
	isMonetaryCol := float64(monetaryCount)/float64(len(values)) > 0.7

	if isMonetaryCol {
		dataType = "monetary"
	} else if isNumericCol {
		dataType = "numeric"
	}

	return map[string]interface{}{
		"unique_count":      uniqueCount,
		"unique_percentage": uniquePercentage,
		"data_type":         dataType,
		"sample_values":     sampleValues,
		"is_numeric":        isNumericCol,
		"is_monetary":       isMonetaryCol,
		"total_values":      len(values),
	}
}

// identifyEntityIDCandidates identifies potential EntityID columns
func identifyEntityIDCandidates(headers []string, columnAnalysis map[string]map[string]interface{}) []interface{} {
	candidates := make([]interface{}, 0)

	// Priority patterns for EntityID columns
	idPatterns := []string{
		"(?i)(transaction[_\\s]*id|txn[_\\s]*id)",
		"(?i)(entity[_\\s]*id|ent[_\\s]*id)",
		"(?i)(instance[_\\s]*id|inst[_\\s]*id)",
		"(?i)(reference[_\\s]*(number|no|num|id)|ref[_\\s]*(no|num|id))",
		"(?i)(receipt[_\\s]*(number|no|num|id))",
		"(?i)(order[_\\s]*(number|no|num|id))",
		"(?i)^id$",
		"(?i)(unique[_\\s]*id)",
	}

	type candidate struct {
		columnName       string
		confidence       float64
		reason           string
		uniquePercentage float64
	}

	var candidateList []candidate

	for _, columnName := range headers {
		analysis := columnAnalysis[columnName]
		uniquePercentage := analysis["unique_percentage"].(float64)

		confidence := 0.0
		reason := ""

		// Check naming patterns (highest priority)
		for i, pattern := range idPatterns {
			if matched, _ := regexp.MatchString(pattern, columnName); matched {
				confidence = 0.95 - float64(i)*0.05 // Decrease confidence for lower priority patterns
				reason = "ID-like naming pattern with high uniqueness"
				break
			}
		}

		// Check uniqueness-based scoring
		if confidence == 0.0 && uniquePercentage >= 95.0 {
			confidence = 0.80
			reason = "Very high uniqueness suggests unique identifier"
		} else if confidence == 0.0 && uniquePercentage >= 85.0 {
			confidence = 0.70
			reason = "High uniqueness suggests potential identifier"
		}

		// Boost confidence for high uniqueness even with pattern match
		if confidence > 0.0 && uniquePercentage >= 98.0 {
			confidence = minFloat(confidence+0.1, 0.98)
		}

		// Only include candidates with reasonable confidence and uniqueness
		if confidence >= 0.60 && uniquePercentage >= 75.0 {
			candidateList = append(candidateList, candidate{
				columnName:       columnName,
				confidence:       confidence,
				reason:           reason,
				uniquePercentage: uniquePercentage,
			})
		}
	}

	// Sort by confidence (highest first)
	for i := 0; i < len(candidateList)-1; i++ {
		for j := i + 1; j < len(candidateList); j++ {
			if candidateList[j].confidence > candidateList[i].confidence {
				candidateList[i], candidateList[j] = candidateList[j], candidateList[i]
			}
		}
	}

	// Convert to expected format
	for _, candidate := range candidateList {
		candidates = append(candidates, map[string]interface{}{
			"column_name":       candidate.columnName,
			"unique_percentage": candidate.uniquePercentage,
			"confidence":        candidate.confidence,
			"reason":            candidate.reason,
		})
	}

	return candidates
}

// identifyAmountCandidates identifies potential Amount columns
func identifyAmountCandidates(headers []string, columnAnalysis map[string]map[string]interface{}) []interface{} {
	candidates := make([]interface{}, 0)

	// Priority patterns for Amount columns
	amountPatterns := []string{
		"(?i)^amount$",
		"(?i)(net[_\\s]*amount|netamount)",
		"(?i)(gross[_\\s]*amount|grossamount)",
		"(?i)(total[_\\s]*amount|totalamount)",
		"(?i)(transaction[_\\s]*amount|txn[_\\s]*amount)",
		"(?i)(payment[_\\s]*amount)",
		"(?i)(balance)",
		"(?i)(value)",
		"(?i)(price)",
		"(?i)(sum)",
		"(?i).*amount.*",
		"(?i).*_amt$",
		"(?i).*amt_.*",
	}

	type candidate struct {
		columnName   string
		confidence   float64
		reason       string
		sampleValues []string
	}

	var candidateList []candidate

	for _, columnName := range headers {
		analysis := columnAnalysis[columnName]
		isMonetary := analysis["is_monetary"].(bool)
		isNumeric := analysis["is_numeric"].(bool)
		sampleValues := analysis["sample_values"].([]string)

		confidence := 0.0
		reason := ""

		// Check naming patterns with monetary data
		for i, pattern := range amountPatterns {
			if matched, _ := regexp.MatchString(pattern, columnName); matched {
				if isMonetary {
					confidence = 0.95 - float64(i)*0.03
					reason = "Amount-like naming with monetary values"
				} else if isNumeric {
					confidence = 0.80 - float64(i)*0.03
					reason = "Amount-like naming with numeric values"
				} else {
					confidence = 0.60 - float64(i)*0.03
					reason = "Amount-like naming pattern"
				}
				break
			}
		}

		// Check data characteristics without pattern match
		if confidence == 0.0 && isMonetary {
			confidence = 0.75
			reason = "Contains monetary values (currency format)"
		} else if confidence == 0.0 && isNumeric {
			confidence = 0.60
			reason = "Contains numeric values"
		}

		// Only include candidates with reasonable confidence
		if confidence >= 0.50 {
			candidateList = append(candidateList, candidate{
				columnName:   columnName,
				confidence:   confidence,
				reason:       reason,
				sampleValues: sampleValues,
			})
		}
	}

	// Sort by confidence (highest first)
	for i := 0; i < len(candidateList)-1; i++ {
		for j := i + 1; j < len(candidateList); j++ {
			if candidateList[j].confidence > candidateList[i].confidence {
				candidateList[i], candidateList[j] = candidateList[j], candidateList[i]
			}
		}
	}

	// Convert to expected format
	for _, candidate := range candidateList {
		candidates = append(candidates, map[string]interface{}{
			"column_name":   candidate.columnName,
			"sample_values": candidate.sampleValues,
			"confidence":    candidate.confidence,
			"reason":        candidate.reason,
		})
	}

	return candidates
}

// isNumeric checks if a string represents a numeric value
func isNumeric(s string) bool {
	// Remove common non-numeric characters that might be in monetary values
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, "$", "")
	s = strings.ReplaceAll(s, "₹", "")
	s = strings.ReplaceAll(s, "€", "")
	s = strings.ReplaceAll(s, "£", "")
	s = strings.TrimSpace(s)

	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// isMonetary checks if a string represents a monetary value
func isMonetary(s string) bool {
	// Check for currency symbols
	currencyPattern := `(?i)[$₹€£¥¢]|\b(usd|eur|gbp|inr|jpy)\b`
	if matched, _ := regexp.MatchString(currencyPattern, s); matched {
		return true
	}

	// Check for decimal pattern with 2 decimal places (common for currency)
	decimalPattern := `^\d+\.\d{2}$`
	if matched, _ := regexp.MatchString(decimalPattern, s); matched {
		return true
	}

	// Check for comma-separated thousands with decimal
	commaPattern := `^\d{1,3}(,\d{3})*(\.\d{2})?$`
	if matched, _ := regexp.MatchString(commaPattern, s); matched {
		return true
	}

	// For simple numeric values, check if they're reasonable monetary amounts
	cleanValue := strings.ReplaceAll(s, ",", "")
	if val, err := strconv.ParseFloat(cleanValue, 64); err == nil {
		// Consider values between 0.01 and 999999999.99 as potentially monetary
		return val >= 0.01 && val <= 999999999.99 &&
			(strings.Contains(s, ".") || len(cleanValue) <= 8)
	}

	return false
}

// minFloat returns the minimum of two float64 values
func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// makeReconSaaSAPICall makes authenticated API calls to recon-saas service
func makeReconSaaSAPICall(ctx context.Context, method, endpoint string, payload interface{}) (map[string]interface{}, error) {
	const baseURL = "https://recon-saas.dev.razorpay.in"
	const authHeader = "Basic cmVjb24tc2FhczpyZWNvbi1zYWFz"

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	var reqBody io.Reader
	if payload != nil {
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %v", err)
		}
		reqBody = bytes.NewReader(payloadBytes)
	}

	req, err := http.NewRequestWithContext(ctx, method, baseURL+endpoint, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("API call failed: %v", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(responseBody))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	return result, nil
}

// createMasterSource creates a master source via recon-saas API
func createMasterSource(ctx context.Context, name, columnsJSON, entityIDColumn, amountColumn string) (string, error) {
	var columns []string
	if err := json.Unmarshal([]byte(columnsJSON), &columns); err != nil {
		return "", fmt.Errorf("invalid columns JSON: %v", err)
	}

	// Generate source schema (all columns as string type)
	sourceSchema := make([]map[string]string, len(columns))
	for i, col := range columns {
		sourceSchema[i] = map[string]string{
			"name": col,
			"type": "string",
		}
	}

	// Generate mapping config with snake_case destinations
	mappingConfig := make([]map[string]string, len(columns))
	for i, col := range columns {
		var destination string
		switch col {
		case entityIDColumn:
			destination = "EntityID"
		case amountColumn:
			destination = "Amount"
		default:
			// Convert to snake_case
			destination = strings.ToLower(strings.ReplaceAll(col, " ", "_"))
			destination = strings.ReplaceAll(destination, "-", "_")
		}

		mappingConfig[i] = map[string]string{
			"value":       "",
			"source":      col,
			"destination": destination,
		}
	}

	payload := map[string]interface{}{
		"name": name,
		"config": map[string]interface{}{
			"is_internal":       false,
			"unique_keys":       []string{entityIDColumn},
			"allow_upload":      false,
			"ingest_to_db":      true,
			"skip_top_rows":     0,
			"split_file_basis":  "",
			"report_enrichment": true,
			"sub_source_config": map[string]interface{}{
				"amount_key":                 "",
				"aggregate_config":           nil,
				"grouping_columns":           nil,
				"sub_master_source_id":       "",
				"enable_sub_source_creation": false,
			},
			"row_hash_value_based_split_config": map[string]interface{}{
				"column_joiner":                    "",
				"header_hash_to_master_source_map": nil,
			},
		},
		"source_schema":  sourceSchema,
		"mapping_config": mappingConfig,
	}

	result, err := makeReconSaaSAPICall(ctx, "POST", "/v1/admin-recon-saas/sources/create", payload)
	if err != nil {
		return "", err
	}

	if id, ok := result["id"].(string); ok {
		return id, nil
	}

	return fmt.Sprintf("mock_master_source_%d", time.Now().Unix()), nil
}

// createMerchantSource creates a merchant source via recon-saas API
func createMerchantSource(ctx context.Context, merchantID, masterSourceID, name string) (string, error) {
	payload := map[string]interface{}{
		"name":             name,
		"merchant_id":      merchantID,
		"master_source_id": masterSourceID,
		"config": map[string]interface{}{
			"cc_emails":          nil,
			"bcc_emails":         nil,
			"allow_upload":       true,
			"reporting_emails":   nil,
			"split_file_basis":   "",
			"beam_sftp_push_job": "",
			"row_hash_value_based_split_config": map[string]interface{}{
				"column_joiner":                    "",
				"header_hash_to_master_source_map": nil,
			},
		},
		"source_schema":  nil,
		"mapping_config": nil,
	}

	result, err := makeReconSaaSAPICall(ctx, "POST", "/v1/admin-recon-saas/sources/create_merchant", payload)
	if err != nil {
		return "", err
	}

	if id, ok := result["id"].(string); ok {
		return id, nil
	}

	return fmt.Sprintf("mock_merchant_source_%d", time.Now().Unix()), nil
}

// createReconStates creates reconciliation states via recon-saas API
func createReconStates(ctx context.Context, merchantID, source1Name, source2Name string) (map[string]interface{}, error) {
	states := []map[string]interface{}{
		{
			"name":     "Reconciled",
			"priority": 2,
			"remarks":  "success",
		},
		{
			"name":     "Unreconciled",
			"priority": 3,
			"remarks":  "Amount mismatch",
		},
		{
			"name":     "Unreconciled",
			"priority": 3,
			"remarks":  fmt.Sprintf("Record not found in %s", source1Name),
		},
		{
			"name":     "Unreconciled",
			"priority": 3,
			"remarks":  fmt.Sprintf("Record not found in %s", source2Name),
		},
	}

	results := make(map[string]interface{})
	stateNames := []string{"reconciled_state", "amount_mismatch_state", "missing_file1_state", "missing_file2_state"}

	for i, state := range states {
		payload := map[string]interface{}{
			"merchant_id": merchantID,
			"name":        state["name"],
			"priority":    state["priority"],
			"remarks":     state["remarks"],
		}

		result, err := makeReconSaaSAPICall(ctx, "POST", "/v1/admin-recon-saas/recon_state", payload)
		if err != nil {
			return nil, fmt.Errorf("failed to create state %s: %v", stateNames[i], err)
		}

		var stateID string
		if id, ok := result["id"].(string); ok {
			stateID = id
		} else {
			stateID = fmt.Sprintf("mock_recon_state_%d_%d", time.Now().Unix(), i)
		}

		results[stateNames[i]] = map[string]interface{}{
			"recon_state_id": stateID,
			"name":           state["name"],
			"priority":       state["priority"],
			"remarks":        state["remarks"],
			"api_response":   result,
		}
	}

	return results, nil
}

// createReconRules creates reconciliation rules via recon-saas API
func createReconRules(ctx context.Context, merchantID, masterSourceID1, masterSourceID2 string, reconStates map[string]interface{}) (map[string]interface{}, error) {
	// Extract recon state IDs
	getStateID := func(stateName string) string {
		if state, ok := reconStates[stateName].(map[string]interface{}); ok {
			if stateID, ok := state["recon_state_id"].(string); ok {
				return stateID
			}
		}
		return fmt.Sprintf("mock_state_%s", stateName)
	}

	rules := []map[string]interface{}{
		{
			"name":           "Reconciled Transactions",
			"expression":     fmt.Sprintf("%s.EntityID == %s.EntityID && %s.Amount.Equal(%s.Amount)", masterSourceID1, masterSourceID2, masterSourceID1, masterSourceID2),
			"recon_state_id": getStateID("reconciled_state"),
			"type":           "reconciled",
		},
		{
			"name":           "Amount Mismatch Transactions",
			"expression":     fmt.Sprintf("%s.EntityID == %s.EntityID && !%s.Amount.Equal(%s.Amount)", masterSourceID1, masterSourceID2, masterSourceID1, masterSourceID2),
			"recon_state_id": getStateID("amount_mismatch_state"),
			"type":           "amount_mismatch",
		},
		{
			"name":           "Missing Record in File 1",
			"expression":     "NoRecord.Value == true",
			"recon_state_id": getStateID("missing_file1_state"),
			"type":           "missing_record",
		},
		{
			"name":           "Missing Record in File 2",
			"expression":     "NoRecord.Value == true",
			"recon_state_id": getStateID("missing_file2_state"),
			"type":           "missing_record",
		},
	}

	results := make(map[string]interface{})
	ruleNames := []string{"reconciled_rule", "amount_mismatch_rule", "missing_record_rule_file1", "missing_record_rule_file2"}

	for i, rule := range rules {
		payload := map[string]interface{}{
			"merchant_id":    merchantID,
			"name":           rule["name"],
			"expression":     rule["expression"],
			"sources":        []string{masterSourceID1, masterSourceID2},
			"recon_state_id": rule["recon_state_id"],
			"type":           "recon",
			"case_rule":      false,
		}

		result, err := makeReconSaaSAPICall(ctx, "POST", "/v1/admin-recon-saas/rule", payload)
		if err != nil {
			return nil, fmt.Errorf("failed to create rule %s: %v", ruleNames[i], err)
		}

		var ruleID string
		if id, ok := result["id"].(string); ok {
			ruleID = id
		} else {
			ruleID = fmt.Sprintf("mock_rule_%d_%d", time.Now().Unix(), i)
		}

		results[ruleNames[i]] = map[string]interface{}{
			"rule_id":        ruleID,
			"name":           rule["name"],
			"expression":     rule["expression"],
			"recon_state_id": rule["recon_state_id"],
			"sources":        []string{masterSourceID1, masterSourceID2},
			"api_response":   result,
		}
	}

	return results, nil
}

// createLookup creates a lookup configuration via recon-saas API
func createLookup(ctx context.Context, merchantID, source1Name, source2Name string) (string, error) {
	payload := map[string]interface{}{
		"name":        fmt.Sprintf("Entity Lookup for %s and %s", source1Name, source2Name),
		"merchant_id": merchantID,
		"config": []map[string]interface{}{
			{
				"source":  "record_internal",
				"columns": []string{"EntityID"},
			},
		},
	}

	result, err := makeReconSaaSAPICall(ctx, "POST", "/v1/admin-recon-saas/lookup", payload)
	if err != nil {
		return "", err
	}

	if id, ok := result["id"].(string); ok {
		return id, nil
	}

	return fmt.Sprintf("mock_lookup_%d", time.Now().Unix()), nil
}

// createMasterReconProcess creates a master reconciliation process via recon-saas API
func createMasterReconProcess(ctx context.Context, source1Name, source2Name, lookupID, masterSourceID1, masterSourceID2 string, ruleIDs []string) (string, error) {
	processName := fmt.Sprintf("%s to %s Reconciliation", source1Name, source2Name)
	productID := fmt.Sprintf("%s_%s",
		strings.ToUpper(strings.ReplaceAll(source1Name[:3], " ", "")),
		strings.ToUpper(strings.ReplaceAll(source2Name[:3], " ", "")))

	payload := map[string]interface{}{
		"name":       processName,
		"product_id": productID,
		"type":       "Gateway",
		"lookup_config": []map[string]interface{}{
			{
				"config": map[string]string{
					masterSourceID1: lookupID,
				},
				"streaming_source_id": masterSourceID2,
			},
		},
		"rules": map[string]interface{}{
			"rule_ids": ruleIDs,
		},
		"sources":  []string{masterSourceID1, masterSourceID2},
		"sequence": []interface{}{},
		"report_config": map[string]interface{}{
			"frontend_cols": []string{
				"Store No_", "Date", "Receipt No_", "NetAmount", "GrossAmount", "Instance ID", "Order Taker",
				"Transaction ID", "Amount", "Description", "Balance", "Reference",
			},
			"source_report_config": []map[string]interface{}{
				{
					"column_map": []map[string]string{
						{"report_column": "Store No_", "source_column": "store_no"},
						{"report_column": "Date", "source_column": "date"},
						{"report_column": "Receipt No_", "source_column": "receipt_no"},
						{"report_column": "NetAmount", "source_column": "Amount"},
						{"report_column": "GrossAmount", "source_column": "grossamount"},
						{"report_column": "Instance ID", "source_column": "EntityID"},
						{"report_column": "Order Taker", "source_column": "order_taker"},
					},
					"master_source_id": masterSourceID1,
				},
				{
					"column_map": []map[string]string{
						{"report_column": "Transaction ID", "source_column": "EntityID"},
						{"report_column": "Date", "source_column": "date"},
						{"report_column": "Amount", "source_column": "Amount"},
						{"report_column": "Description", "source_column": "description"},
						{"report_column": "Balance", "source_column": "balance"},
						{"report_column": "Reference", "source_column": "reference"},
					},
					"master_source_id": masterSourceID2,
				},
			},
		},
	}

	result, err := makeReconSaaSAPICall(ctx, "POST", "/v1/admin-recon-saas/recon_process/master", payload)
	if err != nil {
		return "", err
	}

	if id, ok := result["id"].(string); ok {
		return id, nil
	}

	return fmt.Sprintf("mock_master_recon_process_%d", time.Now().Unix()), nil
}

// createMerchantReconProcess creates a merchant reconciliation process via recon-saas API
func createMerchantReconProcess(ctx context.Context, merchantID, masterReconProcessID, merchantSourceID1, merchantSourceID2 string) (string, error) {
	payload := map[string]interface{}{
		"merchant_id":             merchantID,
		"master_recon_process_id": masterReconProcessID,
		"sources":                 []string{merchantSourceID1, merchantSourceID2},
	}

	result, err := makeReconSaaSAPICall(ctx, "POST", "/v1/admin-recon-saas/recon_process/merchant", payload)
	if err != nil {
		return "", err
	}

	if id, ok := result["id"].(string); ok {
		return id, nil
	}

	return fmt.Sprintf("mock_merchant_recon_process_%d", time.Now().Unix()), nil
}

// extractStateIDs extracts recon state IDs from recon states map
func extractStateIDs(reconStates map[string]interface{}) map[string]string {
	stateIDs := make(map[string]string)

	for stateName, stateData := range reconStates {
		if state, ok := stateData.(map[string]interface{}); ok {
			if stateID, ok := state["recon_state_id"].(string); ok {
				stateIDs[stateName] = stateID
			}
		}
	}

	return stateIDs
}

// extractRuleIDs extracts rule IDs from rules map
func extractRuleIDs(rules map[string]interface{}) map[string]string {
	ruleIDs := make(map[string]string)

	for ruleName, ruleData := range rules {
		if rule, ok := ruleData.(map[string]interface{}); ok {
			if ruleID, ok := rule["rule_id"].(string); ok {
				ruleIDs[ruleName] = ruleID
			}
		}
	}

	return ruleIDs
}
