package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

const (
	apiServerBase   = "https://212.15.215.31:6443"
	targetNamespace = "teuto-portal-clusters"
	fieldManager    = "kubectl-client-side-apply"
)

var customerIDPrefix = regexp.MustCompile(`^(\d+)-`)

type workerGroupInput struct {
	Name        string `yaml:"name"`
	Size        string `yaml:"size"`
	Replicas    int    `yaml:"replicas"`
	MaxReplicas int    `yaml:"maxReplicas"`
}

type clusterInput struct {
	Name         string             `yaml:"name"`
	WorkerGroups []workerGroupInput `yaml:"workerGroups"`
	Version      string             `yaml:"version"`
}

func parseVersion(v string) (major, minor, patch int, err error) {
	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return 0, 0, 0, fmt.Errorf("expected version as major.minor.patch, got %q", v)
	}
	nums := make([]int, 3)
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid version segment %q: %w", p, err)
		}
		nums[i] = n
	}
	return nums[0], nums[1], nums[2], nil
}

func customerIDFromName(name string) int {
	m := customerIDPrefix.FindStringSubmatch(name)
	if m == nil {
		return 0
	}
	id, _ := strconv.Atoi(m[1])
	return id
}

func buildHelmRelease(in clusterInput, name string) (map[string]any, error) {
	major, minor, patch, err := parseVersion(in.Version)
	if err != nil {
		return nil, err
	}

	nodePools := map[string]any{}
	for _, wg := range in.WorkerGroups {
		pool := map[string]any{
			"flavor":   wg.Size,
			"replicas": wg.Replicas,
		}
		if wg.MaxReplicas != wg.Replicas {
			pool["maxReplicas"] = wg.MaxReplicas
		}
		nodePools[wg.Name] = pool
	}

	return map[string]any{
		"apiVersion": "helm.toolkit.fluxcd.io/v2",
		"kind":       "HelmRelease",
		"metadata": map[string]any{
			"name":      name,
			"namespace": targetNamespace,
			"labels": map[string]any{
				"t8s.teuto.net/managed-by": "teuto-portal",
				"t8s.teuto.net/role":       "workload",
			},
		},
		"spec": map[string]any{
			"interval": "1m",
			"chart": map[string]any{
				"spec": map[string]any{
					"chart":             "t8s-cluster",
					"reconcileStrategy": "ChartVersion",
					"version":           "9.x.x",
					"sourceRef": map[string]any{
						"kind":      "HelmRepository",
						"name":      "teuto-net",
						"namespace": "flux-system",
					},
				},
			},
			"driftDetection": map[string]any{
				"mode": "enabled",
			},
			"values": map[string]any{
				"cloud": "ffm3-dev",
				"controlPlane": map[string]any{
					"hosted": true,
				},
				"metadata": map[string]any{
					"customerID":            customerIDFromName(in.Name),
					"customerName":          "scs",
					"friendlyName":          in.Name,
					"serviceLevelAgreement": "None",
				},
				"nodePools": nodePools,
				"version": map[string]any{
					"major": major,
					"minor": minor,
					"patch": patch,
				},
			},
		},
	}, nil
}

func handleClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		http.Error(w, "missing Authorization header", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	var in clusterInput
	if err := yaml.Unmarshal(body, &in); err != nil {
		http.Error(w, "invalid input: "+err.Error(), http.StatusBadRequest)
		return
	}

	name := uuid.NewString()

	helmRelease, err := buildHelmRelease(in, name)
	if err != nil {
		http.Error(w, "conversion failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	payload, err := json.Marshal(helmRelease)
	if err != nil {
		http.Error(w, "failed to marshal converted body: "+err.Error(), http.StatusInternalServerError)
		return
	}

	targetURL := fmt.Sprintf(
		"%s/apis/helm.toolkit.fluxcd.io/v2/namespaces/%s/helmreleases?fieldManager=%s&fieldValidation=Strict",
		apiServerBase, targetNamespace, fieldManager,
	)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(payload))
	if err != nil {
		http.Error(w, "failed to build upstream request: "+err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authHeader)

	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "upstream request failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "failed to read upstream response: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(respBody)
}

func handleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		http.Error(w, "missing Authorization header", http.StatusUnauthorized)
		return
	}

	id := r.PathValue("uuid")
	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, "cluster identifier must be a UUID, not a friendly name", http.StatusBadRequest)
		return
	}

	targetURL := fmt.Sprintf(
		"%s/apis/helm.toolkit.fluxcd.io/v2/namespaces/%s/helmreleases/%s",
		apiServerBase, targetNamespace, id,
	)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	req, err := http.NewRequest(http.MethodDelete, targetURL, nil)
	if err != nil {
		http.Error(w, "failed to build upstream request: "+err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Authorization", authHeader)

	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "upstream request failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "failed to read upstream response: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(respBody)
}

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	flag.Parse()

	http.HandleFunc("/clusters", handleClusters)
	http.HandleFunc("DELETE /clusters/{uuid}", handleDeleteCluster)

	log.Printf("listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
