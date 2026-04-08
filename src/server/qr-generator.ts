import QRCode from 'qrcode';

export async function generateQrSvg(data: string, opts: { size?: number; fgColor?: string; bgColor?: string } = {}): Promise<string> {
  const { size = 256, fgColor = '#000000', bgColor = '#FFFFFF' } = opts;
  return QRCode.toString(data, {
    type: 'svg',
    width: size,
    color: { dark: fgColor, light: bgColor },
    errorCorrectionLevel: 'M',
  });
}

export async function generateQrDataUrl(data: string, opts: { size?: number; fgColor?: string; bgColor?: string } = {}): Promise<string> {
  const { size = 256, fgColor = '#000000', bgColor = '#FFFFFF' } = opts;
  return QRCode.toDataURL(data, {
    width: size,
    color: { dark: fgColor, light: bgColor },
    errorCorrectionLevel: 'M',
  });
}

export async function generateQrBuffer(data: string, opts: { size?: number; fgColor?: string; bgColor?: string } = {}): Promise<Buffer> {
  const { size = 256, fgColor = '#000000', bgColor = '#FFFFFF' } = opts;
  return QRCode.toBuffer(data, {
    width: size,
    color: { dark: fgColor, light: bgColor },
    errorCorrectionLevel: 'M',
  });
}