FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files and install dependencies
COPY package.json package-lock.json ./
RUN npm install

# Copy the rest of the application code
COPY . .

# Build the React application
RUN npm run build

# Use Nginx to serve the built files
FROM nginx:alpine

# Copy the built files from the builder stage
COPY --from=builder /app/build /usr/share/nginx/html

# Copy the Nginx configuration template
# Nginx docker image supports templates in this directory and will 
# automatically substitute environment variables and output to /etc/nginx/conf.d
COPY nginx/default.conf.template /etc/nginx/templates/default.conf.template

# Default GATEWAY_URI (can be overridden at runtime)
ENV GATEWAY_URI=http://localhost:8585

# Expose port 80
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
