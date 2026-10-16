# Owner actions

Steps only the owner can do, because they need hardware this build machine does not have.

## 1. Real GPU, single job end to end (RTX 4060) — BLOCKED here

Needs a Linux machine with the RTX 4060, the NVIDIA driver, Docker, and the NVIDIA Container Toolkit.

1. Check the toolkit: `docker run --rm --gpus all ubuntu:24.04 nvidia-smi` must print the GPU.
2. Build and run the check:
   ```sh
   cd nebula-gpu-cloud
   cargo build --release
   scripts/owner_gpu_check.sh
   ```
3. Expected output:
   * step 1 prints one line like `0, NVIDIA GeForce RTX 4060, 8188, GPU-...`;
   * step 2 shows the node with `"model":"NVIDIA GeForce RTX 4060"`, `memory_mb` and `uuid`;
   * step 3 shows the job `succeeded`, a log line `GPU 0: NVIDIA GeForce RTX 4060 (UUID: GPU-...)`, `CUDA_VISIBLE_DEVICES=0 NEBULA_HOST_GPUS=0`, and finally `PASS: real GPU job ran in a container`.
4. Save: commit the folder `docs/evidence/owner-gpu-<date>/` that the script writes. Copy the real line from `nvidia-smi-query.csv` into the fixture `ONE_4060` in `src/gpu.rs` (replacing the hand-written one) and run `cargo test --release gpu::`.
5. Then change the requirements row "GPU pass-through to containers" to VERIFIED with a link to that folder.

If step 3 fails with a permissions error inside the container, retry with `--container-user` set to a uid in the `video` group, and record that in the evidence.
