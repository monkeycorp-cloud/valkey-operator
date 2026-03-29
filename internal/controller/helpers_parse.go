package controller

import (
	"fmt"
	"strconv"
	"strings"
)

// stripVerbatimPrefix removes the RESP3 verbatim string encoding prefix if
// present. When using HELLO 3, Valkey returns bulk strings like CLUSTER INFO,
// CLUSTER NODES, and CLUSTER MYID as verbatim strings: the raw value starts
// with a 3-char encoding type followed by ":" (e.g. "txt:"). This prefix must
// be stripped before parsing the actual content.
func stripVerbatimPrefix(s string) string {
	if len(s) > 4 && s[3] == ':' && !strings.Contains(s[:3], "\n") {
		return s[4:]
	}
	return s
}

// parseClusterInfoField extracts a string field from CLUSTER INFO output.
func parseClusterInfoField(info, field string) string {
	info = stripVerbatimPrefix(info)
	prefix := field + ":"
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix))
		}
	}
	return ""
}

// parseClusterInfoInt extracts an integer field from CLUSTER INFO output.
func parseClusterInfoInt(info, field string) int64 {
	info = stripVerbatimPrefix(info)
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, field+":") {
			val := strings.TrimSpace(strings.TrimPrefix(line, field+":"))
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return 0
			}
			return n
		}
	}
	return 0
}

// parseIntField extracts an integer value from an INFO output line like "field:value".
func parseIntField(info, field string) int64 {
	info = stripVerbatimPrefix(info)
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, field) {
			val := strings.TrimSpace(strings.TrimPrefix(line, field))
			var n int64
			if _, err := fmt.Sscanf(val, "%d", &n); err != nil {
				return -1
			}
			return n
		}
	}
	return -1
}

// splitAddrIP extracts the IP portion from a CLUSTER NODES address field
// (the part before @busport). Handles both IPv4 ("10.0.0.1:6379") and IPv6
// ("[fe80::1]:6379") by splitting on the last colon.
func splitAddrIP(atPart string) string {
	if lastColon := strings.LastIndex(atPart, ":"); lastColon >= 0 {
		return atPart[:lastColon]
	}
	return atPart
}

// parseClusterNodes parses the output of CLUSTER NODES into a slice of clusterNodeState.
//
// CLUSTER NODES addr field formats:
//   - Without announce:         ip:port@busport
//   - With announce-hostname:   ip:port@busport,hostname
//
// When cluster-announce-ip is set, the ip portion is always the announced IP.
// When cluster-announce-hostname is set, the hostname appears after the comma.
func parseClusterNodes(nodesRaw string) []clusterNodeState {
	nodesRaw = stripVerbatimPrefix(nodesRaw)
	var nodes []clusterNodeState
	for _, line := range strings.Split(nodesRaw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 8 {
			continue
		}
		// Format: <id> <ip:port@bus[,hostname]> <flags> <master> <ping> <pong> <epoch> <state> [slots...]
		addrRaw := parts[1]
		atPart := strings.Split(addrRaw, "@")[0] // ip:port
		ip := splitAddrIP(atPart)

		// Hostname is the optional suffix after the comma in the bus part: ip:port@busport,hostname
		var hostname string
		if atIdx := strings.Index(addrRaw, "@"); atIdx >= 0 {
			busPart := addrRaw[atIdx+1:]
			if commaIdx := strings.Index(busPart, ","); commaIdx >= 0 {
				// Extract hostname between first and second comma in bus part
				after := busPart[commaIdx+1:]
				if nextComma := strings.Index(after, ","); nextComma >= 0 {
					after = after[:nextComma]
				}
				hostname = after
			}
		}

		slots := ""
		if len(parts) > 8 {
			slots = strings.Join(parts[8:], " ")
		}
		nodes = append(nodes, clusterNodeState{
			id:       parts[0],
			ip:       ip,
			hostname: hostname,
			flags:    parts[2],
			masterID: parts[3],
			slots:    slots,
		})
	}
	return nodes
}

// parseNodeRoles parses CLUSTER NODES and returns entries indexed by both IP and hostname.
// Supports clusters with or without cluster-announce-hostname.
//
// addr field formats:
//   - ip:port@busport           (no announce-hostname)
//   - ip:port@busport,hostname  (with announce-hostname)
func parseNodeRoles(nodesRaw string) []nodeRoleEntry {
	nodesRaw = stripVerbatimPrefix(nodesRaw)
	var entries []nodeRoleEntry
	for _, line := range strings.Split(nodesRaw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 3 {
			continue
		}
		addrRaw := parts[1]
		atPart := strings.Split(addrRaw, "@")[0]
		ip := splitAddrIP(atPart)

		var hostname string
		if atIdx := strings.Index(addrRaw, "@"); atIdx >= 0 {
			busPart := addrRaw[atIdx+1:]
			if commaIdx := strings.Index(busPart, ","); commaIdx >= 0 {
				// Extract hostname between first and second comma in bus part
				after := busPart[commaIdx+1:]
				if nextComma := strings.Index(after, ","); nextComma >= 0 {
					after = after[:nextComma]
				}
				hostname = after
			}
		}

		flags := parts[2]
		var role string
		switch {
		case strings.Contains(flags, "master"):
			role = rolePrimary
		case strings.Contains(flags, "slave"):
			role = roleReplica
		default:
			continue
		}
		entries = append(entries, nodeRoleEntry{role: role, ip: ip, hostname: hostname})
	}
	return entries
}

// parseReplicaAddrsForMaster returns IP and hostname sets of nodes that are
// slaves of the given masterID. Supports clusters with or without announce-hostname.
func parseReplicaAddrsForMaster(nodesRaw, masterNodeID string) (ips map[string]struct{}, hostnames map[string]struct{}) {
	nodesRaw = stripVerbatimPrefix(nodesRaw)
	ips = make(map[string]struct{})
	hostnames = make(map[string]struct{})
	for _, line := range strings.Split(nodesRaw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 4 {
			continue
		}
		if !strings.Contains(parts[2], "slave") || parts[3] != masterNodeID {
			continue
		}
		addrRaw := parts[1]
		atPart := strings.Split(addrRaw, "@")[0]
		ip := splitAddrIP(atPart)
		if ip != "" {
			ips[ip] = struct{}{}
		}
		if atIdx := strings.Index(addrRaw, "@"); atIdx >= 0 {
			busPart := addrRaw[atIdx+1:]
			if commaIdx := strings.Index(busPart, ","); commaIdx >= 0 {
				// Extract hostname between first and second comma in bus part
				after := busPart[commaIdx+1:]
				if nextComma := strings.Index(after, ","); nextComma >= 0 {
					after = after[:nextComma]
				}
				if after != "" {
					hostnames[after] = struct{}{}
				}
			}
		}
	}
	return
}

// buildKnownAddrSets returns two sets — IPs and hostnames — of all nodes
// currently known to the cluster from a CLUSTER NODES output.
func buildKnownAddrSets(nodesRaw string) (ips map[string]struct{}, hostnames map[string]struct{}) {
	nodesRaw = stripVerbatimPrefix(nodesRaw)
	ips = make(map[string]struct{})
	hostnames = make(map[string]struct{})
	for _, line := range strings.Split(nodesRaw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		addrRaw := parts[1]
		atPart := strings.Split(addrRaw, "@")[0]
		ip := splitAddrIP(atPart)
		if ip != "" {
			ips[ip] = struct{}{}
		}
		if atIdx := strings.Index(addrRaw, "@"); atIdx >= 0 {
			busPart := addrRaw[atIdx+1:]
			if commaIdx := strings.Index(busPart, ","); commaIdx >= 0 {
				// Extract hostname between first and second comma in bus part
				after := busPart[commaIdx+1:]
				if nextComma := strings.Index(after, ","); nextComma >= 0 {
					after = after[:nextComma]
				}
				if after != "" {
					hostnames[after] = struct{}{}
				}
			}
		}
	}
	return
}
