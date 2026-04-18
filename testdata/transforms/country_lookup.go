// transform: country_lookup
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/higino/eulantir/internal/connector"
)

// countryByCode maps international dial codes to country names.
var countryByCode = map[string]string{
	"1":   "United States",
	"7":   "Russia",
	"20":  "Egypt",
	"27":  "South Africa",
	"30":  "Greece",
	"31":  "Netherlands",
	"32":  "Belgium",
	"33":  "France",
	"34":  "Spain",
	"36":  "Hungary",
	"39":  "Italy",
	"40":  "Romania",
	"41":  "Switzerland",
	"43":  "Austria",
	"44":  "United Kingdom",
	"45":  "Denmark",
	"46":  "Sweden",
	"47":  "Norway",
	"48":  "Poland",
	"49":  "Germany",
	"51":  "Peru",
	"52":  "Mexico",
	"54":  "Argentina",
	"55":  "Brazil",
	"56":  "Chile",
	"57":  "Colombia",
	"58":  "Venezuela",
	"61":  "Australia",
	"62":  "Indonesia",
	"63":  "Philippines",
	"64":  "New Zealand",
	"65":  "Singapore",
	"66":  "Thailand",
	"81":  "Japan",
	"82":  "South Korea",
	"86":  "China",
	"90":  "Turkey",
	"91":  "India",
	"92":  "Pakistan",
	"93":  "Afghanistan",
	"98":  "Iran",
	"212": "Morocco",
	"213": "Algeria",
	"216": "Tunisia",
	"218": "Libya",
	"220": "Gambia",
	"234": "Nigeria",
	"254": "Kenya",
	"255": "Tanzania",
	"256": "Uganda",
	"260": "Zambia",
	"263": "Zimbabwe",
	"351": "Portugal",
	"352": "Luxembourg",
	"353": "Ireland",
	"354": "Iceland",
	"358": "Finland",
	"359": "Bulgaria",
	"370": "Lithuania",
	"371": "Latvia",
	"372": "Estonia",
	"380": "Ukraine",
	"381": "Serbia",
	"385": "Croatia",
	"386": "Slovenia",
	"420": "Czech Republic",
	"421": "Slovakia",
	"503": "El Salvador",
	"506": "Costa Rica",
	"507": "Panama",
	"591": "Bolivia",
	"593": "Ecuador",
	"595": "Paraguay",
	"598": "Uruguay",
	"966": "Saudi Arabia",
	"971": "United Arab Emirates",
	"972": "Israel",
	"975": "Bhutan",
	"977": "Nepal",
}

// CountryLookup maps the "country_code" field (a dial code string like "55")
// to a human-readable "country_name" field (e.g. "Brazil").
// If the code is unknown the record is passed through unchanged.
// Map: always returns exactly one record.
func CountryLookup(ctx context.Context, in connector.Record) ([]connector.Record, error) {
	var row map[string]any
	if err := json.Unmarshal(in.Value, &row); err != nil {
		return nil, fmt.Errorf("CountryLookup: unmarshal: %w", err)
	}

	if code, ok := row["country_code"].(string); ok {
		if name, found := countryByCode[code]; found {
			row["country_name"] = name
		} else {
			row["country_name"] = "Unknown"
		}
	}

	modified, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("CountryLookup: marshal: %w", err)
	}

	out := in
	out.Value = modified
	return []connector.Record{out}, nil
}
