# The pinned batteries-included runtime supplies Chromium and Debian trixie libraries.
FROM apache/superset:7.0.0rc2@sha256:570bb49b69374c8e7b827f1b210b2a5f491fbc632e56eb2beb4a1b3ca5b5b38f AS upstream-runtime

# Full build images include compilers without an apt install during the build.
FROM node:24-trixie@sha256:016f604a11c4c725f0a4c62e29ff11adab153d297bf97046c9bf2ddb9f1011d3 AS node-builder
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
    PUPPETEER_SKIP_DOWNLOAD=true \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
RUN git config --global url."https://github.com/".insteadOf ssh://git@github.com/

# Webpack loads simple-zstd through the proxy configuration even in production.
FROM node-builder AS zstd-builder
RUN curl -fsSL https://codeload.github.com/facebook/zstd/tar.gz/f8745da6ff1ad1e7bab384bd1f9d742439278e99 -o /tmp/zstd.tar.gz && \
    echo "4b0bd1f0cfb25e61b9103c35f27395530ff5b4c0d2513a00fd745849e85ea52c  /tmp/zstd.tar.gz" | sha256sum -c - && \
    mkdir /tmp/zstd-src && \
    tar -xzf /tmp/zstd.tar.gz -C /tmp/zstd-src --strip-components=1 && \
    make -C /tmp/zstd-src/programs -j2 HAVE_ZLIB=0 HAVE_LZMA=0 HAVE_LZ4=0 LDFLAGS=-static zstd && \
    install /tmp/zstd-src/programs/zstd /usr/local/bin/zstd

FROM node-builder AS frontend
WORKDIR /app/superset-frontend
# npm ci must see workspace manifests to install the Aven chart dependencies.
COPY superset-frontend ./
# This mirror carries the canonical SheetJS tarball. Verify its bytes against the
# unchanged lockfile before putting it in npm's content-addressed offline cache.
RUN --mount=type=cache,target=/root/.npm \
    node -e 'const fs = require("fs"); const crypto = require("crypto"); (async () => { const expected = require("./package-lock.json").packages["node_modules/xlsx"].integrity; const response = await fetch("https://raw.githubusercontent.com/sheetxl/vendor-xlsx/4ffbd44d59452385b09171776c7ed3ea31d4cda1/xlsx-0.20.3.tgz"); if (!response.ok) throw new Error(`SheetJS archive HTTP ${response.status}`); const data = Buffer.from(await response.arrayBuffer()); if (`sha512-${crypto.createHash("sha512").update(data).digest("base64")}` !== expected) throw new Error("SheetJS archive integrity mismatch"); fs.writeFileSync("/tmp/xlsx.tgz", data); })().catch(error => { console.error(error); process.exit(1); });' && \
    npm cache add /tmp/xlsx.tgz && \
    npm ci --prefer-offline && \
    rm /tmp/xlsx.tgz
COPY --from=zstd-builder /usr/local/bin/zstd /usr/local/bin/zstd
COPY superset/translations /app/superset/translations
RUN mkdir -p /app/superset/static/assets && npm run build
# Generate translations without running source formatters in the image build.
RUN for file in /app/superset/translations/*/LC_MESSAGES/*.po; do \
      ./node_modules/.bin/po2json --domain superset --format jed1.x --fuzzy "$file" "${file%.po}.json"; \
    done

FROM node-builder AS websocket
WORKDIR /app/superset-websocket
COPY superset-websocket/package.json superset-websocket/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci
COPY superset-websocket ./
RUN npm run build

# LDAP/SASL libraries in both images are the same Debian trixie package versions.
# Only build headers and the matching SASL linker file come from the runtime;
# the compiled extension links to the runtime's own shared libraries.
FROM python:3.11.14-trixie@sha256:3b3706a90cb23f04fabb0d255824f9a70ceb46177041898133dd5a35f3a50f0a AS ldap-wheel
COPY --from=upstream-runtime /usr/include/sasl /usr/include/sasl
COPY --from=upstream-runtime /usr/lib/x86_64-linux-gnu/libsasl2.so /usr/lib/x86_64-linux-gnu/libsasl2.so
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip wheel --wheel-dir=/wheels python-ldap==3.4.4

FROM upstream-runtime AS superset
USER root
WORKDIR /app
ARG SUPERSET_GIT_SHA
LABEL org.opencontainers.image.revision=${SUPERSET_GIT_SHA}

# Replace upstream application code and assets, not just selected fork patches.
RUN rm -rf superset superset-core apache_superset.egg-info
COPY pyproject.toml setup.py MANIFEST.in README.md LICENSE.txt NOTICE ./
COPY requirements/base.txt requirements/base.txt
COPY scripts scripts
COPY superset-core superset-core
COPY superset superset
COPY superset-frontend/package.json superset-frontend/package.json
COPY --chmod=755 docker/*.sh /app/docker/
COPY --chmod=755 docker/entrypoints /app/docker/entrypoints
COPY --chmod=755 docker/entrypoints/run-server.sh /usr/bin/run-server.sh
COPY --from=frontend /app/superset/static/assets superset/static/assets
COPY --from=frontend /app/superset/static/service-worker.js superset/static/service-worker.js
COPY --from=frontend /app/superset/translations superset/translations
COPY --from=websocket /usr/local/bin/node /usr/local/bin/node
COPY --from=websocket /app/superset-websocket/dist /app/superset-websocket/dist

RUN --mount=type=bind,from=ldap-wheel,source=/wheels,target=/wheels \
    --mount=type=cache,target=/app/superset_home/.cache/uv \
    uv pip install --no-index --find-links=/wheels python-ldap==3.4.4 && \
    uv pip install -r requirements/base.txt \
      -e ".[postgres,clickhouse,snowflake,gsheets,gsheets-export]" \
      snowflake-sqlalchemy==1.11.1 snowflake-connector-python==4.8.0 \
      opentelemetry-api==1.29.0 opentelemetry-sdk==1.29.0 && \
    find superset/translations -name '*.po' -delete && \
    python -m compileall /app/superset

# setup.py cannot discover Git metadata because .git is excluded from builds.
RUN python -c 'import json, pathlib, sys; path = pathlib.Path("/app/superset/static/version_info.json"); info = json.loads(path.read_text()); info["GIT_SHA"] = sys.argv[1]; path.write_text(json.dumps(info))' "$SUPERSET_GIT_SHA"
USER superset
