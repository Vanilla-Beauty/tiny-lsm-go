#! /usr/bin/sh

# ? abandoned after using `.github/workflows/deploy.yml` for deployment

# Check if at least one argument is provided
if [ -z "$1" ]; then
  echo "Error: No commit message provided."
  exit 1
fi

rm -rf book
rm -rf ../tiny-lsm-go-web/book

mdbook build

mv book/ ../tiny-lsm-go-web/
cd ../tiny-lsm-go-web
git add -A
git commit -m "$1"
git push origin gh-pages