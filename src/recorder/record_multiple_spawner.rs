use cpal::traits::DeviceTrait;
use crossbeam_channel::Receiver;

use super::{
    Recorder,
    constants::{CustomSample, ResampleTargetStream, TargetFormat},
    errors::AudioRecorderError,
};

impl Recorder {
    pub fn record_multiple<T, U>(
        &mut self,
        input_device: cpal::Device,
        output_device: cpal::Device,
    ) -> Result<Receiver<Vec<TargetFormat>>, AudioRecorderError>
    where
        T: CustomSample + 'static,
        U: CustomSample + 'static,
    {
        tracing::debug!("Record multiple started");

        let input_config = input_device.default_input_config().map_err(|e| {
            tracing::error!("Failed to get input config: {}", e);
            AudioRecorderError::DeviceError("Failed to get input config")
        })?;
        let output_config = output_device.default_output_config().map_err(|e| {
            tracing::error!("Failed to get output config: {}", e);
            AudioRecorderError::DeviceError("Failed to get output config")
        })?;

        let input_sample_rate = input_config.sample_rate() as usize;
        let output_sample_rate = output_config.sample_rate() as usize;
        
        if self.target_sample_rate.is_none() {
            self.target_sample_rate = Some(input_sample_rate.min(output_sample_rate) as u32);
        }
        if self.channels.is_none() {
            self.channels = Some(2);
        }
        if self.sample_size.is_none() {
            self.sample_size = Some(input_config.sample_format().sample_size() as u32);
        }

        let target_rate = self.target_sample_rate.unwrap() as usize;
        let resample_target = if target_rate == input_sample_rate && target_rate == output_sample_rate {
            ResampleTargetStream::None
        } else if target_rate == input_sample_rate {
            ResampleTargetStream::Output
        } else if target_rate == output_sample_rate {
            ResampleTargetStream::Input
        } else {
            ResampleTargetStream::Both
        };

        tracing::debug!("Resample strategy: {:?}, target_rate: {}", resample_target, target_rate);
        tracing::debug!("Config: {:?}", self);

        match resample_target {
            ResampleTargetStream::None => {
                self.without_resampler::<T, U>(input_device, output_device)
            }
            ResampleTargetStream::Output => {
                let origin_rate = output_sample_rate;
                self.with_output_resampler::<T, U>(
                    input_device,
                    output_device,
                    target_rate,
                    origin_rate,
                )
            }
            ResampleTargetStream::Input => {
                let origin_rate = input_sample_rate;
                self.with_input_resampler::<T, U>(
                    input_device,
                    output_device,
                    target_rate,
                    origin_rate,
                )
            }
            ResampleTargetStream::Both => {
                self.with_both_resampler::<T, U>(
                    input_device,
                    output_device,
                    target_rate,
                    input_sample_rate,
                    output_sample_rate,
                )
            }
        }
    }
}
