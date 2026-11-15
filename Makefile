export GOTOOLCHAIN ?= go1.27.1

.PHONY: check release-check

check:
	@files="$$(gofmt -l .)" || exit $$?; \
	if [ -n "$$files" ]; then echo "gofmt failed:"; echo "$$files"; exit 1; fi
	@grep -nx 'var Version = "dev"' internal/app/server.go >/dev/null \
		|| { echo "internal/app/server.go: var Version must remain \"dev\""; exit 1; }
	go vet ./...
	go test ./... -count=1
	go build -buildvcs=false -o /dev/null .

# 发版门禁：make release-check TAG=v5.0.0
# 1) TAG 必须是严格 SemVer vX.Y.Z（拦 -magpie.N / 预发布后缀 / 缺 v）
# 2) Version 必须保持可注入的 "dev"
# 3) CHANGELOG.md 必须有对应 ## X.Y.Z 小节且含非空实质内容
# 4) 若本地已存在该 tag，必须是 annotated tag（git cat-file -t == "tag"）
release-check: export TAG_VAL = $(TAG)
release-check:
	@awk 'BEGIN { s = ENVIRON["TAG_VAL"]; if (s == "") { print "usage: make release-check TAG=vX.Y.Z" > "/dev/stderr"; exit 1 } if (s ~ /[\r\n]/ || s !~ /^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$$/) { print "TAG is not strict single-line SemVer vX.Y.Z: " s > "/dev/stderr"; exit 1 } }'
	@grep -nx 'var Version = "dev"' internal/app/server.go >/dev/null \
		|| { echo "internal/app/server.go: var Version must remain \"dev\"" >&2; exit 1; }
	@ver="$${TAG_VAL#v}"; \
	awk -v ver="$$ver" ' \
		$$0 == "## " ver || index($$0, "## " ver " ") == 1 { inf=1; body=0; next } \
		inf && /^## / { exit } \
		inf { \
			if ($$0 ~ /^[[:space:]]*$$/) next; \
			if ($$0 ~ /^###/) next; \
			body=1; exit \
		} \
		END { \
			if (!inf) { print "CHANGELOG.md: missing section for " ver > "/dev/stderr"; exit 1 } \
			if (!body) { print "CHANGELOG.md: section " ver " has no substantive content" > "/dev/stderr"; exit 1 } \
		}' CHANGELOG.md || exit $$?
	@if git rev-parse -q --verify "refs/tags/$$TAG_VAL" >/dev/null 2>&1; then \
		[ "$$(git cat-file -t "refs/tags/$$TAG_VAL")" = "tag" ] \
			|| { echo "tag $$TAG_VAL exists but is lightweight; release tags must be annotated" >&2; exit 1; }; \
	fi
