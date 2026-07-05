package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckConfigCommandLoadsConfigAndExits(t *testing.T) {
	configPath := writeCommandConfig(t, `{
		"emby_base_url": "http://127.0.0.1:8096",
		"cache_dir": "`+filepath.ToSlash(filepath.Join(t.TempDir(), "cache"))+`"
	}`)
	cmd := exec.Command("go", "run", ".", "--config", configPath, "--check-config")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\n%s", err, output)
	}
	if string(output) != "config ok\n" {
		t.Fatalf("output=%q", output)
	}
}

func TestCheckConfigCommandRejectsInvalidConfig(t *testing.T) {
	configPath := writeCommandConfig(t, `{
		"emby_base_url": "http://127.0.0.1:8096",
		"cache_dir": "`+filepath.ToSlash(filepath.Join(t.TempDir(), "cache"))+`",
		"listen_port": 70000
	}`)
	cmd := exec.Command("go", "run", ".", "--config", configPath, "--check-config")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("command unexpectedly succeeded: %s", output)
	}
	if !strings.Contains(string(output), "load config failed:") {
		t.Fatalf("output=%q", output)
	}
}

func TestPrintEffectiveConfigRedactsSecretAndIncludesDefaults(t *testing.T) {
	cacheDir := filepath.ToSlash(filepath.Join(t.TempDir(), "cache"))
	configPath := writeCommandConfig(t, `{
		"emby_base_url": "http://127.0.0.1:8096/",
		"cache_dir": "`+cacheDir+`",
		"prewarm_api_key": "internal-secret",
		"rollout": {
			"enabled": true,
			"item_allowlist": ["10535"]
		}
	}`)
	cmd := exec.Command("go", "run", ".", "--config", configPath, "--print-effective-config")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\n%s", err, output)
	}
	if strings.Contains(string(output), "internal-secret") {
		t.Fatalf("effective config leaked secret: %s", output)
	}
	var payload map[string]any
	if err := json.Unmarshal(output, &payload); err != nil {
		t.Fatalf("effective config json: %v\n%s", err, output)
	}
	if payload["emby_base_url"] != "http://127.0.0.1:8096" {
		t.Fatalf("emby_base_url=%v", payload["emby_base_url"])
	}
	if payload["fallback_base_url"] != "http://127.0.0.1:8096" {
		t.Fatalf("fallback_base_url=%v", payload["fallback_base_url"])
	}
	if payload["listen_host"] != "127.0.0.1" || payload["listen_port"].(float64) != 18180 {
		t.Fatalf("listen=%v:%v", payload["listen_host"], payload["listen_port"])
	}
	if payload["prewarm_api_key"] != "REDACTED" {
		t.Fatalf("prewarm_api_key=%v", payload["prewarm_api_key"])
	}
	prewarm := payload["prewarm"].(map[string]any)
	if prewarm["interval_seconds"].(float64) != 900 {
		t.Fatalf("prewarm.interval_seconds=%v", prewarm["interval_seconds"])
	}
	if prewarm["playback_info_timeout_seconds"].(float64) != 15 {
		t.Fatalf("prewarm.playback_info_timeout_seconds=%v", prewarm["playback_info_timeout_seconds"])
	}
	session := payload["session"].(map[string]any)
	if session["state_db"] != filepath.ToSlash(filepath.Join(cacheDir, "state", "phase2.sqlite3")) {
		t.Fatalf("session.state_db=%v", session["state_db"])
	}
	rollout := payload["rollout"].(map[string]any)
	items := rollout["item_allowlist"].([]any)
	if len(items) != 1 || items[0] != "10535" {
		t.Fatalf("rollout.item_allowlist=%v", rollout["item_allowlist"])
	}
}

func TestPrintEffectiveConfigCanShowSecret(t *testing.T) {
	configPath := writeCommandConfig(t, `{
		"emby_base_url": "http://127.0.0.1:8096",
		"cache_dir": "`+filepath.ToSlash(filepath.Join(t.TempDir(), "cache"))+`",
		"prewarm_api_key": "internal-secret"
	}`)
	cmd := exec.Command("go", "run", ".", "--config", configPath, "--print-effective-config", "--show-secrets")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %v\n%s", err, output)
	}
	var payload map[string]any
	if err := json.Unmarshal(output, &payload); err != nil {
		t.Fatalf("effective config json: %v\n%s", err, output)
	}
	if payload["prewarm_api_key"] != "internal-secret" {
		t.Fatalf("prewarm_api_key=%v", payload["prewarm_api_key"])
	}
}

func writeCommandConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
