package server

import (
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/xflash-panda/acl-engine/pkg/acl"
)

// TestACLRulesMatching tests all ACL rules from config.yaml
func TestACLRulesMatching(t *testing.T) {
	// ACL rules from config.yaml
	aclRulesText := `reject(all, udp/443)
warp(all, tcp/22)
warp(all, tcp/25)
warp(all, tcp/465)
warp(all, tcp/587)
warp(all, tcp/993)
warp(all, tcp/995)
warp(all, tcp/3389)
warp(geosite:openai)
warp(suffix:google.com)
warp(geosite:google-deepmind)
warp(geosite:sb)
warp(geosite:category-porn)
warp(geosite:netflix)
warp(geosite:disney)
warp(geosite:hbo)
warp(geosite:bahamut)
warp(geosite:tvb)
warp(geosite:tiktok)
warp(geosite:imgur)
warp(geosite:reddit)
warp(suffix:ping0.cc)
direct(all)`

	rules, err := acl.ParseTextRules(aclRulesText)
	if err != nil {
		t.Fatalf("Failed to parse rules: %v", err)
	}

	// Create outbounds map (just use string names for testing)
	outbounds := map[string]string{
		"warp":   "warp",
		"direct": "direct",
		"reject": "reject",
	}

	// Create GeoLoader
	geoLoader := &acl.AutoGeoLoader{
		DataDir:       "/tmp/anytls-agent-node",
		GeoIPFormat:   acl.GeoIPFormatMMDB,
		GeoSiteFormat: acl.GeoSiteFormatSing,
		GeoIPURL:      acl.MetaCubeXGeoIPMMDBURL,
		GeoSiteURL:    acl.MetaCubeXGeoSiteDBURL,
		Logger: func(format string, args ...interface{}) {
			t.Logf(format, args...)
		},
	}

	// Compile rules
	ruleSet, err := acl.Compile(rules, outbounds, 1024, geoLoader)
	if err != nil {
		t.Fatalf("Failed to compile rules: %v", err)
	}

	// Test cases
	testCases := []struct {
		host        string
		port        uint16
		proto       acl.Protocol
		expectedOut string
		description string
	}{
		// Port-based rules (TCP)
		{"example.com", 22, acl.ProtocolTCP, "warp", "SSH port should use warp"},
		{"example.com", 25, acl.ProtocolTCP, "warp", "SMTP port should use warp"},
		{"example.com", 465, acl.ProtocolTCP, "warp", "SMTPS port should use warp"},
		{"example.com", 587, acl.ProtocolTCP, "warp", "SMTP submission port should use warp"},
		{"example.com", 993, acl.ProtocolTCP, "warp", "IMAPS port should use warp"},
		{"example.com", 995, acl.ProtocolTCP, "warp", "POP3S port should use warp"},
		{"example.com", 3389, acl.ProtocolTCP, "warp", "RDP port should use warp"},

		// Port-based rules (UDP) - should be rejected
		{"example.com", 443, acl.ProtocolUDP, "reject", "UDP/443 should be rejected"},

		// Geosite rules - OpenAI (note: subdomain matching depends on geosite data)
		{"openai.com", 443, acl.ProtocolTCP, "warp", "OpenAI root should use warp"},

		// Suffix rules - Google
		{"www.google.com", 443, acl.ProtocolTCP, "warp", "Google.com suffix should use warp"},
		{"mail.google.com", 443, acl.ProtocolTCP, "warp", "mail.google.com should use warp"},
		{"drive.google.com", 443, acl.ProtocolTCP, "warp", "drive.google.com should use warp"},
		{"google.com", 443, acl.ProtocolTCP, "warp", "google.com root should use warp"},

		// ip.sb (geosite:sb) - THIS IS THE KEY TEST
		{"ip.sb", 443, acl.ProtocolTCP, "warp", "ip.sb should use warp (geosite:sb)"},
		{"ip.sb", 80, acl.ProtocolTCP, "warp", "ip.sb:80 should use warp (geosite:sb)"},

		// Netflix
		{"netflix.com", 443, acl.ProtocolTCP, "warp", "Netflix root should use warp"},

		// Disney
		{"disneyplus.com", 443, acl.ProtocolTCP, "warp", "Disney+ root should use warp"},

		// TikTok
		{"tiktok.com", 443, acl.ProtocolTCP, "warp", "TikTok root should use warp"},

		// Reddit
		{"reddit.com", 443, acl.ProtocolTCP, "warp", "Reddit root should use warp"},

		// Imgur
		{"imgur.com", 443, acl.ProtocolTCP, "warp", "Imgur should use warp"},

		// Custom suffix - ping0.cc
		{"ping0.cc", 443, acl.ProtocolTCP, "warp", "ping0.cc should use warp"},
		{"www.ping0.cc", 443, acl.ProtocolTCP, "warp", "www.ping0.cc should use warp"},

		// Default direct
		{"example.com", 443, acl.ProtocolTCP, "direct", "example.com should use direct"},
		{"baidu.com", 443, acl.ProtocolTCP, "direct", "baidu.com should use direct"},
		{"qq.com", 443, acl.ProtocolTCP, "direct", "qq.com should use direct"},
		{"taobao.com", 443, acl.ProtocolTCP, "direct", "taobao.com should use direct"},

		// IP addresses (no domain match, should use direct)
		{"8.8.8.8", 443, acl.ProtocolTCP, "direct", "IP address should use direct"},
		{"1.1.1.1", 53, acl.ProtocolTCP, "direct", "Cloudflare DNS IP should use direct"},
		{"34.117.59.81", 443, acl.ProtocolTCP, "direct", "ip.sb's IP should use direct (not warp!)"},
	}

	// Print report header
	fmt.Println("\n" + strings.Repeat("=", 110))
	fmt.Println("ACL Rules Test Report")
	fmt.Println(strings.Repeat("=", 110))
	fmt.Printf("%-45s | %-6s | %-5s | %-8s | %-8s | %s\n",
		"Host", "Port", "Proto", "Expected", "Actual", "Status")
	fmt.Println(strings.Repeat("-", 110))

	passed := 0
	failed := 0

	for _, tc := range testCases {
		var hostInfo acl.HostInfo
		if ip := net.ParseIP(tc.host); ip != nil {
			hostInfo = acl.HostInfo{IPv4: ip.To4()}
			if hostInfo.IPv4 == nil {
				hostInfo.IPv6 = ip
			}
		} else {
			hostInfo = acl.HostInfo{Name: tc.host}
		}

		actualOut, _ := ruleSet.Match(hostInfo, tc.proto, tc.port)

		protoStr := "TCP"
		if tc.proto == acl.ProtocolUDP {
			protoStr = "UDP"
		}

		status := "✓ PASS"
		if actualOut != tc.expectedOut {
			status = "✗ FAIL"
			failed++
		} else {
			passed++
		}

		fmt.Printf("%-45s | %-6d | %-5s | %-8s | %-8s | %s\n",
			tc.host, tc.port, protoStr, tc.expectedOut, actualOut, status)

		// Also use t.Errorf for test framework
		if actualOut != tc.expectedOut {
			t.Errorf("%s: expected %s, got %s", tc.description, tc.expectedOut, actualOut)
		}
	}

	// Print summary
	fmt.Println(strings.Repeat("-", 110))
	fmt.Printf("Total: %d | Passed: %d | Failed: %d\n", passed+failed, passed, failed)
	fmt.Println(strings.Repeat("=", 110))

	// Print conclusion
	fmt.Println("\n" + strings.Repeat("=", 110))
	fmt.Println("CONCLUSION")
	fmt.Println(strings.Repeat("=", 110))
	if failed == 0 {
		fmt.Println("✓ All ACL rules are working correctly!")
		fmt.Println("")
		fmt.Println("IMPORTANT: geosite rules (like geosite:sb) only match DOMAIN NAMES, not IP addresses.")
		fmt.Println("If your proxy client sends the destination as an IP address instead of a domain name,")
		fmt.Println("the geosite rules will NOT match, and traffic will fall through to 'direct(all)'.")
		fmt.Println("")
		fmt.Println("This is likely why ip.sb appears to 'work' without going through warp:")
		fmt.Println("  - If client sends 'ip.sb:443' -> matches geosite:sb -> uses warp")
		fmt.Println("  - If client sends '34.117.59.81:443' -> no geosite match -> uses direct")
	} else {
		fmt.Println("✗ Some ACL rules failed!")
	}
	fmt.Println(strings.Repeat("=", 110))
}

// TestIPvsDomainMatching specifically demonstrates the IP vs Domain issue
func TestIPvsDomainMatching(t *testing.T) {
	aclRulesText := `warp(geosite:sb)
direct(all)`

	rules, err := acl.ParseTextRules(aclRulesText)
	if err != nil {
		t.Fatalf("Failed to parse rules: %v", err)
	}

	outbounds := map[string]string{
		"warp":   "warp",
		"direct": "direct",
	}

	geoLoader := &acl.AutoGeoLoader{
		DataDir:       "/tmp/anytls-agent-node",
		GeoIPFormat:   acl.GeoIPFormatMMDB,
		GeoSiteFormat: acl.GeoSiteFormatSing,
		GeoIPURL:      acl.MetaCubeXGeoIPMMDBURL,
		GeoSiteURL:    acl.MetaCubeXGeoSiteDBURL,
	}

	ruleSet, err := acl.Compile(rules, outbounds, 1024, geoLoader)
	if err != nil {
		t.Fatalf("Failed to compile rules: %v", err)
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("IP vs Domain Matching Test")
	fmt.Println(strings.Repeat("=", 80))

	// Domain test
	domainHost := acl.HostInfo{Name: "ip.sb"}
	domainOut, _ := ruleSet.Match(domainHost, acl.ProtocolTCP, 443)
	fmt.Printf("Domain 'ip.sb:443'           -> %s\n", domainOut)

	// IP test (ip.sb's actual IP)
	ipHost := acl.HostInfo{IPv4: net.ParseIP("34.117.59.81").To4()}
	ipOut, _ := ruleSet.Match(ipHost, acl.ProtocolTCP, 443)
	fmt.Printf("IP '34.117.59.81:443'        -> %s\n", ipOut)

	fmt.Println(strings.Repeat("-", 80))
	fmt.Println("Result:")
	fmt.Printf("  - Domain 'ip.sb' matches geosite:sb? %v (output: %s)\n", domainOut == "warp", domainOut)
	fmt.Printf("  - IP '34.117.59.81' matches geosite:sb? %v (output: %s)\n", ipOut == "warp", ipOut)
	fmt.Println(strings.Repeat("=", 80))

	if domainOut != "warp" {
		t.Errorf("Domain ip.sb should match warp, got %s", domainOut)
	}
	if ipOut != "direct" {
		t.Errorf("IP address should fallback to direct, got %s", ipOut)
	}
}
