package app

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// Task-mode pre-bunking regression via the stable HTTP entrypoint.
//
// Chat-mode prompts pre-bunk the platform's own "built-in tools 0 remaining"
// notice so the model keeps emitting ```tool_call``` blocks. Task mode takes a
// separate web Task channel where that notice never appears, so the pre-bunking
// paragraph must be skipped there; a stray "the platform may warn you" line in
// a task prompt would invent a warning that does not exist. This test drives
// the public /v1beta/models/{model}:generateContent handler (not the internal
// prompt builder) so the assertion fails at the HTTP boundary on a baseline
// that lacks the task-mode skip.
func TestGeminiAPIToolPromptTailSkipsPlatformNoticeForTaskMode(t *testing.T) {
	// Task models only resolve with a signed-in cookie (modelNeedsLogin);
	// withPoolCookie inserts one pool account so all six models resolve.
	withPoolCookie(t)

	const body = `{"contents":[{"role":"user","parts":[{"text":"list files then read README"}]}],
	  "tools":[{"functionDeclarations":[{"name":"run_command","description":"run a shell command"}]}]}`

	// Anchors every tool prompt tail must keep (format contract, both modes).
	anchors := []string{
		"this is the ONLY execution channel",
		"Act, do not explain",
		"Do not repeat a finished step",
		"emit the tool_call block now",
	}
	// Chat-only platform-notice pre-bunking; must be absent in task mode.
	immunity := []string{
		"Heads-up — platform notice",
		"0 remaining",
		"summarize now",
		"tool limit was reached",
	}

	models := []struct {
		name     string
		taskMode bool
	}{
		{"gemini-4-carbon", false},
		{"gemini-4-neon", false},
		{"gemini-4-hydrogen", false},
		{"gemini-4-carbon-task", true},
		{"gemini-4-neon-task", true},
		{"gemini-4-hydrogen-task", true},
	}

	for _, m := range models {
		seen := stubGeminiCall(t, "ok", nil, &StreamResult{}, nil)
		w := httptest.NewRecorder()
		handleGeminiModelItem(w, httptest.NewRequest("POST",
			"/v1beta/models/"+m.name+":generateContent", strings.NewReader(body)))
		if w.Code != 200 {
			t.Fatalf("%s: status %d: %s", m.name, w.Code, w.Body)
		}
		prompt := seen.prompt
		if prompt == "" {
			t.Fatalf("%s: upstream prompt not captured", m.name)
		}
		for _, want := range anchors {
			if !strings.Contains(prompt, want) {
				t.Errorf("%s: prompt missing tool anchor %q\n---\n%s", m.name, want, prompt)
			}
		}
		if m.taskMode {
			for _, banned := range immunity {
				if strings.Contains(prompt, banned) {
					t.Errorf("%s: task-mode prompt must not contain platform notice %q\n---\n%s",
						m.name, banned, prompt)
				}
			}
		} else {
			for _, want := range immunity {
				if !strings.Contains(prompt, want) {
					t.Errorf("%s: chat-mode prompt missing platform notice %q\n---\n%s",
						m.name, want, prompt)
				}
			}
		}
	}
}
