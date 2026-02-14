package storage

import (
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	backendUploadTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"storage.backend.upload",
		"Duration of backend uploads",
		"Total bytes uploaded via backend",
		"Total backend uploads",
	))
	backendDownloadTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"storage.backend.download",
		"Duration of backend downloads",
		"Total bytes downloaded via backend",
		"Total backend downloads",
	))
	backendRangeGetTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"storage.backend.range_get",
		"Duration of backend range gets",
		"Total bytes range-read via backend",
		"Total backend range gets",
	))
	backendSizeTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"storage.backend.size",
		"Duration of backend size queries",
		"Total bytes (virt sizes) returned via backend size",
		"Total backend size queries",
	))
	backendDeleteTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"storage.backend.delete",
		"Duration of backend deletes",
		"Total bytes deleted via backend (always 0)",
		"Total backend deletes",
	))
)
