package paths

import (
	"path"
)

func buildPath(cacheScope string, saveType string, file string) string {
	return path.Join(cacheScope, saveType, file)
}

func GetLayerFilesCachePath(cacheScope string, hash string) string {
	return buildPath(cacheScope, "files", hash+".tar")
}

func HashToPath(cacheScope, hash string) string {
	return buildPath(cacheScope, "index", hash)
}
