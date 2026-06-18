import Image from "next/image";

export default function Home() {
  return (
    <div className="flex flex-col flex-1 items-center justify-center bg-zinc-50 font-sans dark:bg-black">
      <main className="flex flex-1 w-full flex-col items-center justify-between py-32 px-16 bg-white dark:bg-black sm:items-start">
        <div className="">
          <h1 className="text-4xl font-bold">SDOH Project</h1>
          <p>/London crime as a case study</p>
        </div>
      </main>
    </div>
  );
}
