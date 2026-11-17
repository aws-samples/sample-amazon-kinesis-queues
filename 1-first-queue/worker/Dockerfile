# The consumer is bundled into dist/ with esbuild when CDK synthesizes the stack
# (see lib/bundle-worker.ts), so the image needs only Node.js and one JS file.
FROM public.ecr.aws/docker/library/node:22-slim
WORKDIR /app
ENV NODE_ENV=production
COPY dist/ ./
USER node
CMD ["node", "--enable-source-maps", "main.js"]
