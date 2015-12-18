package linux

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/akutz/gofig"
	"github.com/akutz/goof"
	"github.com/docker/docker/pkg/mount"
	"github.com/opencontainers/runc/libcontainer/label"

	"github.com/emccode/rexray/core"
	"github.com/emccode/rexray/core/errors"
)

const providerName = "linux"

func init() {
	core.RegisterDriver(providerName, newDriver)
	gofig.Register(configRegistration())
}

type driver struct {
	r *core.RexRay
}

func newDriver() core.Driver {
	return &driver{}
}

func (d *driver) Init(r *core.RexRay) error {
	if runtime.GOOS == "linux" {
		d.r = r
		log.WithField("provider", providerName).Info(
			"os driver initialized")
		return nil
	}
	return errors.ErrUnknownOS
}

func (d *driver) Name() string {
	return providerName
}

func (d *driver) GetMounts(
	deviceName, mountPoint string) (core.MountInfoArray, error) {

	mounts, err := mount.GetMounts()
	if err != nil {
		return nil, err
	}

	if mountPoint == "" && deviceName == "" {
		return mounts, nil
	} else if mountPoint != "" && deviceName != "" {
		return nil, goof.New("Cannot specify mountPoint and deviceName")
	}

	var matchedMounts []*mount.Info
	for _, mount := range mounts {
		if mount.Mountpoint == mountPoint || mount.Source == deviceName {
			matchedMounts = append(matchedMounts, mount)
		}
	}
	return matchedMounts, nil
}

func (d *driver) Mounted(mountPoint string) (bool, error) {
	return mount.Mounted(mountPoint)
}

func (d *driver) Unmount(mountPoint string) error {
	return mount.Unmount(mountPoint)
}

func (d *driver) isNfsDevice(device string) bool {
	return strings.Contains(device, ":")
}

func (d *driver) nfsMount(device, target string) error {
	command := exec.Command("mount", device, target)
	output, err := command.CombinedOutput()
	if err != nil {
		return goof.WithError(fmt.Sprintf("failed mounting: %s", output), err)
	}

	return nil
}

func (d *driver) fileModeMountPath() (fileMode os.FileMode) {
	return os.FileMode(d.volumeFileMode())
}

func (d *driver) Mount(
	device, target, mountOptions, mountLabel string) error {

	if d.isNfsDevice(device) {

		if err := d.nfsMount(device, target); err != nil {
			return err
		}

		os.MkdirAll(d.volumeMountPath(target), d.fileModeMountPath())
		os.Chmod(d.volumeMountPath(target), d.fileModeMountPath())

		return nil
	}

	fsType, err := probeFsType(device)
	if err != nil {
		return err
	}

	options := label.FormatMountLabel("", mountLabel)
	options = fmt.Sprintf("%s,%s", mountOptions, mountLabel)
	if fsType == "xfs" {
		options = fmt.Sprintf("%s,nouuid", mountOptions)
	}

	if err := mount.Mount(device, target, fsType, options); err != nil {
		return fmt.Errorf("Couldn't mount directory %s at %s: %s", device, target, err)
	}

	os.MkdirAll(d.volumeMountPath(target), d.fileModeMountPath())
	os.Chmod(d.volumeMountPath(target), d.fileModeMountPath())

	return nil
}

// Format will look for ext4/xfs and overwrite it is it doesn't exist
func (d *driver) Format(
	deviceName, newFsType string, overwriteFs bool) error {

	var fsDetected bool

	fsType, err := probeFsType(deviceName)
	if err != nil && err != errors.ErrUnknownFileSystem {
		return err
	}
	if fsType != "" {
		fsDetected = true
	}

	log.WithFields(log.Fields{
		"fsDetected":  fsDetected,
		"fsType":      fsType,
		"deviceName":  deviceName,
		"overwriteFs": overwriteFs,
		"driverName":  d.Name()}).Info("probe information")

	if overwriteFs || !fsDetected {
		switch newFsType {
		case "ext4":
			if err := exec.Command("mkfs.ext4", "-F", deviceName).Run(); err != nil {
				return fmt.Errorf(
					"Problem creating filesystem on %s with error %s",
					deviceName, err)
			}
		case "xfs":
			if err := exec.Command("mkfs.xfs", "-f", deviceName).Run(); err != nil {
				return fmt.Errorf(
					"Problem creating filesystem on %s with error %s",
					deviceName, err)
			}
		default:
			return goof.New("Unsupported FS")
		}
	}

	return nil
}

// from github.com/docker/docker/daemon/graphdriver/devmapper/
// this should be abstracted outside of graphdriver but within Docker package,
// here temporarily
type probeData struct {
	fsName string
	magic  string
	offset uint64
}

func probeFsType(device string) (string, error) {
	probes := []probeData{
		{"btrfs", "_BHRfS_M", 0x10040},
		{"ext4", "\123\357", 0x438},
		{"xfs", "XFSB", 0},
	}

	maxLen := uint64(0)
	for _, p := range probes {
		l := p.offset + uint64(len(p.magic))
		if l > maxLen {
			maxLen = l
		}
	}

	file, err := os.Open(device)
	if err != nil {
		return "", err
	}
	defer file.Close()

	buffer := make([]byte, maxLen)
	l, err := file.Read(buffer)
	if err != nil {
		return "", err
	}

	if uint64(l) != maxLen {
		return "", fmt.Errorf("unable to detect filesystem type of %s, short read", device)
	}

	for _, p := range probes {
		if bytes.Equal([]byte(p.magic), buffer[p.offset:p.offset+uint64(len(p.magic))]) {
			return p.fsName, nil
		}
	}

	return "", errors.ErrUnknownFileSystem
}

func (d *driver) volumeMountPath(target string) string {
	return fmt.Sprintf("%s%s", target, d.volumeRootPath())
}

func (d *driver) volumeFileMode() int {
	return d.r.Config.GetInt("linux.volume.filemode")
}

func (d *driver) volumeRootPath() string {
	return d.r.Config.GetString("linux.volume.rootpath")
}

func configRegistration() *gofig.Registration {
	r := gofig.NewRegistration("Linux")
	r.Key(gofig.Int, "", 0700, "", "linux.volume.filemode")
	r.Key(gofig.String, "", "/data", "", "linux.volume.rootpath")
	return r
}
