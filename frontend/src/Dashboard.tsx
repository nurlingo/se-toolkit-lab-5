import { useState, useEffect } from 'react'
import { Bar, Line } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
)

const STORAGE_KEY = 'api_key'

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface PassRateEntry {
  task: string
  avg_score: number
  attempts: number
}

const LABS = ['lab-01', 'lab-02', 'lab-03', 'lab-04', 'lab-05']

function Dashboard() {
  const [lab, setLab] = useState('lab-04')
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [timeline, setTimeline] = useState<TimelineEntry[]>([])
  const [passRates, setPassRates] = useState<PassRateEntry[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    const token = localStorage.getItem(STORAGE_KEY) ?? ''
    const headers = { Authorization: `Bearer ${token}` }

    setLoading(true)
    setError('')

    Promise.all([
      fetch(`/analytics/scores?lab=${lab}`, { headers }).then((r) => {
        if (!r.ok) throw new Error(`Scores: HTTP ${r.status}`)
        return r.json() as Promise<ScoreBucket[]>
      }),
      fetch(`/analytics/timeline?lab=${lab}`, { headers }).then((r) => {
        if (!r.ok) throw new Error(`Timeline: HTTP ${r.status}`)
        return r.json() as Promise<TimelineEntry[]>
      }),
      fetch(`/analytics/pass-rates?lab=${lab}`, { headers }).then((r) => {
        if (!r.ok) throw new Error(`Pass rates: HTTP ${r.status}`)
        return r.json() as Promise<PassRateEntry[]>
      }),
    ])
      .then(([scoresData, timelineData, passRatesData]) => {
        setScores(scoresData)
        setTimeline(timelineData)
        setPassRates(passRatesData)
        setLoading(false)
      })
      .catch((err: Error) => {
        setError(err.message)
        setLoading(false)
      })
  }, [lab])

  const scoresChartData = {
    labels: scores.map((s) => s.bucket),
    datasets: [
      {
        label: 'Students',
        data: scores.map((s) => s.count),
        backgroundColor: ['#ef4444', '#f97316', '#eab308', '#22c55e'],
      },
    ],
  }

  const timelineChartData = {
    labels: timeline.map((t) => t.date),
    datasets: [
      {
        label: 'Submissions',
        data: timeline.map((t) => t.submissions),
        borderColor: '#3b82f6',
        backgroundColor: '#3b82f680',
        tension: 0.3,
      },
    ],
  }

  return (
    <div>
      <div style={{ marginBottom: '1rem' }}>
        <label htmlFor="lab-select">Lab: </label>
        <select
          id="lab-select"
          value={lab}
          onChange={(e) => setLab(e.target.value)}
        >
          {LABS.map((l) => (
            <option key={l} value={l}>
              {l}
            </option>
          ))}
        </select>
      </div>

      {loading && <p>Loading...</p>}
      {error && <p>Error: {error}</p>}

      {!loading && !error && (
        <>
          <div style={{ maxWidth: 600, marginBottom: '2rem' }}>
            <h2>Score Distribution</h2>
            <Bar
              data={scoresChartData}
              options={{ plugins: { legend: { display: false } } }}
            />
          </div>

          <div style={{ maxWidth: 600, marginBottom: '2rem' }}>
            <h2>Submissions Over Time</h2>
            <Line data={timelineChartData} />
          </div>

          <div style={{ marginBottom: '2rem' }}>
            <h2>Pass Rates</h2>
            <table>
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Avg Score</th>
                  <th>Attempts</th>
                </tr>
              </thead>
              <tbody>
                {passRates.map((pr) => (
                  <tr key={pr.task}>
                    <td>{pr.task}</td>
                    <td>{pr.avg_score}</td>
                    <td>{pr.attempts}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  )
}

export default Dashboard
