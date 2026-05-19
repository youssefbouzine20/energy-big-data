export default function Layout({ children }: any) {
  return (
    <div className="flex min-h-screen bg-[#070b17] text-white">
      <Sidebar />

      <main className="flex-1 p-6 overflow-auto">
        <Header />

        <div className="mt-6">
          {children}
        </div>
      </main>
    </div>
  )
}