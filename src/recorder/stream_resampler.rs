
pub struct StreamResampler {
    input_rate: f64,
    output_rate: f64,
    channels: usize,
    ratio: f64,
    phase: f64,
    leftover: Vec<f32>,
}

impl StreamResampler {
    pub fn new(input_rate: u32, output_rate: u32, channels: u16) -> Self {
        let channels = channels as usize;
        let leftover = vec![0.0f32; channels];
        Self {
            input_rate: input_rate as f64,
            output_rate: output_rate as f64,
            channels,
            ratio: output_rate as f64 / input_rate as f64,
            phase: 0.0,
            leftover,
        }
    }

    pub fn process(&mut self, input: &[f32]) -> Vec<f32> {
        if input.is_empty() {
            return Vec::new();
        }

        let channels = self.channels;
        let mut output = Vec::new();

        let mut combined = Vec::with_capacity(self.leftover.len() + input.len());
        combined.extend_from_slice(&self.leftover);
        combined.extend_from_slice(input);

        let total_frames = combined.len() / channels;
        while self.phase < total_frames as f64 - 1.0 {
            let idx = self.phase.floor() as usize;
            let frac = self.phase - idx as f64;

            if idx + 1 >= total_frames {
                break;
            }

            for ch in 0..channels {
                let s1 = combined[idx * channels + ch];
                let s2 = combined[(idx + 1) * channels + ch];
                let out = s1 as f64 + frac * (s2 as f64 - s1 as f64);
                output.push(out as f32);
            }

            self.phase += self.ratio;
        }

        let consumed_input_frames = (self.phase.floor() as usize).min(total_frames);
        let remaining_start = consumed_input_frames * channels;
        if remaining_start < combined.len() {
            self.leftover = combined[remaining_start..].to_vec();
        } else {
            self.leftover = vec![0.0f32; channels];
        }

        self.phase -= consumed_input_frames as f64;
        if self.phase < 0.0 {
            self.phase = 0.0;
        }

        output
    }

    pub fn reset(&mut self) {
        self.phase = 0.0;
        self.leftover = vec![0.0f32; self.channels];
    }
}