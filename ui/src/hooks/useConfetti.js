import confetti from 'canvas-confetti';

export const useConfetti = () => {
  return {
    showConfetti: () =>
      confetti({
        particleCount: 100,
        spread: 360,
        origin: {y: 0},
        angle: 270,
        startVelocity: 20,
      }),
  };
};
