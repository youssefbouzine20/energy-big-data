type Props = {
  title: string
  value: string
  subtitle?: string
}

export default function DashboardCard({
  title,
  value,
  subtitle
}: Props) {
  return (
    <div
      className="
      bg-white/5
      border border-white/10
      rounded-3xl
      p-6
      backdrop-blur-xl
      hover:scale-[1.02]
      transition-all
      shadow-[0_0_30px_rgba(139,92,246,0.15)]
      "
    >
      <p className="text-slate-400 text-sm">
        {title}
      </p>

      <h2 className="text-4xl font-bold mt-3">
        {value}
      </h2>

      <p className="text-green-400 mt-2 text-sm">
        {subtitle}
      </p>
    </div>
  )
}