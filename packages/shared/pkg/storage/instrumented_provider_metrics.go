package storage

import (
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	providerGetFrameTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"storage.provider.get_frame",
		"Duration of provider GetFrame calls",
		"Total bytes read via provider GetFrame",
		"Total provider GetFrame calls",
	))
	providerStoreFileTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"storage.provider.store_file",
		"Duration of provider StoreFile calls",
		"Total bytes stored via provider StoreFile",
		"Total provider StoreFile calls",
	))
	providerGetBlobTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"storage.provider.get_blob",
		"Duration of provider GetBlob calls",
		"Total bytes read via provider GetBlob",
		"Total provider GetBlob calls",
	))
	providerCopyBlobTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"storage.provider.copy_blob",
		"Duration of provider CopyBlob calls",
		"Total bytes copied via provider CopyBlob",
		"Total provider CopyBlob calls",
	))
	providerStoreBlobTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"storage.provider.store_blob",
		"Duration of provider StoreBlob calls",
		"Total bytes stored via provider StoreBlob (always 0)",
		"Total provider StoreBlob calls",
	))
	providerSizeTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"storage.provider.size",
		"Duration of provider Size calls",
		"Total bytes (virt sizes) returned via provider Size",
		"Total provider Size calls",
	))
	providerDeleteTimerFactory = utils.Must(telemetry.NewTimerFactory(meter,
		"storage.provider.delete",
		"Duration of provider DeleteWithPrefix calls",
		"Total bytes deleted via provider (always 0)",
		"Total provider DeleteWithPrefix calls",
	))
)
