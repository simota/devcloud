import { useState, useCallback } from "react";
import { AppHeader } from "./components/s3/AppHeader";
import { BucketSidebar } from "./components/s3/BucketSidebar";
import { ObjectBrowser } from "./components/s3/ObjectBrowser";
import { ObjectInspector } from "./components/s3/ObjectInspector";
import { Footer } from "./components/s3/Footer";
import { CreateBucketDialog, DeleteBucketDialog, DeleteObjectDialog } from "./components/s3/Dialogs";
import {
  MOCK_BUCKETS,
  MOCK_OBJECT_STORE,
  MOCK_STATUS,
  INITIAL_ACTIVITY,
  getMockObjectDetail,
} from "./components/s3/mockData";
import { Bucket, ObjectDetail, S3Object, ActivityEntry } from "./components/s3/types";
import { PanelRight, ArrowLeft } from "lucide-react";

export default function App() {
  // Data state
  const [buckets, setBuckets] = useState<Bucket[]>(MOCK_BUCKETS);
  const [selectedBucketName, setSelectedBucketName] = useState<string | null>("demo");
  const [currentPrefix, setCurrentPrefix] = useState<string>("");
  const [selectedObject, setSelectedObject] = useState<ObjectDetail | null>(null);
  const [lastActivity, setLastActivity] = useState<ActivityEntry | null>(INITIAL_ACTIVITY);
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Mobile/tablet state
  const [mobileView, setMobileView] = useState<"buckets" | "objects" | "inspector">("buckets");
  const [isInspectorOpen, setIsInspectorOpen] = useState(false);

  // Dialog state
  const [showCreateBucket, setShowCreateBucket] = useState(false);
  const [deleteBucket, setDeleteBucket] = useState<Bucket | null>(null);
  const [deleteObject, setDeleteObject] = useState<ObjectDetail | null>(null);

  // Current bucket's objects
  const bucketStore = selectedBucketName ? MOCK_OBJECT_STORE[selectedBucketName] : null;
  const prefixData = bucketStore?.[currentPrefix] ?? { commonPrefixes: [], objects: [] };
  const objects = prefixData.objects;
  const commonPrefixes = prefixData.commonPrefixes;

  function handleRefresh() {
    setIsRefreshing(true);
    setTimeout(() => {
      setIsRefreshing(false);
      setLastActivity({
        method: "GET",
        path: `/api/s3/buckets${selectedBucketName ? `/${selectedBucketName}/objects` : ""}`,
        timestamp: new Date().toISOString(),
        statusCode: 200,
      });
    }, 600);
  }

  function handleSelectBucket(name: string) {
    setSelectedBucketName(name);
    setCurrentPrefix("");
    setSelectedObject(null);
    setMobileView("objects");
    setIsInspectorOpen(false);
    setLastActivity({
      method: "GET",
      path: `/api/s3/buckets/${name}/objects`,
      timestamp: new Date().toISOString(),
      statusCode: 200,
    });
  }

  function handleNavigate(prefix: string) {
    setCurrentPrefix(prefix);
    setSelectedObject(null);
    setMobileView("objects");
  }

  function handleSelectObject(obj: S3Object) {
    if (selectedBucketName) {
      const detail = getMockObjectDetail(selectedBucketName, obj.key);
      setSelectedObject(detail);
      setMobileView("inspector");
      setIsInspectorOpen(true);
      setLastActivity({
        method: "HEAD",
        path: `/${selectedBucketName}/${obj.key}`,
        timestamp: new Date().toISOString(),
        statusCode: 200,
      });
    }
  }

  const handleCreateBucket = useCallback((name: string, region: string) => {
    const newBucket: Bucket = {
      name,
      region,
      createdAt: new Date().toISOString(),
      objectCount: 0,
      totalBytes: 0,
      versioning: "Off",
    };
    setBuckets((prev) => [...prev, newBucket]);
    setLastActivity({
      method: "PUT",
      path: `/api/s3/buckets`,
      timestamp: new Date().toISOString(),
      statusCode: 200,
    });
  }, []);

  function handleDeleteBucketConfirm() {
    if (!deleteBucket) return;
    setBuckets((prev) => prev.filter((b) => b.name !== deleteBucket.name));
    if (selectedBucketName === deleteBucket.name) {
      setSelectedBucketName(null);
      setCurrentPrefix("");
      setSelectedObject(null);
    }
    setLastActivity({
      method: "DELETE",
      path: `/api/s3/buckets/${deleteBucket.name}`,
      timestamp: new Date().toISOString(),
      statusCode: 204,
    });
    setDeleteBucket(null);
  }

  function handleDeleteObjectConfirm() {
    if (!deleteObject || !selectedBucketName) return;
    setLastActivity({
      method: "DELETE",
      path: `/${selectedBucketName}/${deleteObject.key}`,
      timestamp: new Date().toISOString(),
      statusCode: 204,
    });
    setBuckets((prev) =>
      prev.map((b) =>
        b.name === selectedBucketName
          ? { ...b, objectCount: Math.max(0, b.objectCount - 1) }
          : b
      )
    );
    setSelectedObject(null);
    setIsInspectorOpen(false);
    setMobileView("objects");
    setDeleteObject(null);
  }

  return (
    <div
      style={{
        height: "100vh",
        display: "flex",
        flexDirection: "column",
        background: "linear-gradient(180deg, #FAFAF9 0%, #F4F4F2 100%)",
        overflow: "hidden",
        fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
        color: "#0A0A0A",
      }}
    >
      {/* Header */}
      <AppHeader
        status={MOCK_STATUS}
        onRefresh={handleRefresh}
        onCreateBucket={() => setShowCreateBucket(true)}
        isRefreshing={isRefreshing}
      />

      {/* Main 3-pane layout */}
      <div style={{ flex: 1, display: "flex", overflow: "hidden", position: "relative" }}>

        {/* ── Bucket Sidebar ── */}
        {/* Desktop: always visible */}
        {/* Tablet/Mobile: toggleable */}
        <div
          className={`bucket-sidebar-wrapper ${mobileView !== "buckets" ? "sidebar-hidden-mobile" : ""}`}
          style={{ display: "flex" }}
        >
          <BucketSidebar
            buckets={buckets}
            selectedBucket={selectedBucketName}
            onSelectBucket={handleSelectBucket}
            onDeleteBucket={setDeleteBucket}
            onCreateBucket={() => setShowCreateBucket(true)}
            onRefresh={handleRefresh}
          />
        </div>

        {/* ── Object Browser ── */}
        <div
          className={`object-browser-wrapper ${mobileView === "buckets" || mobileView === "inspector" ? "browser-hidden-mobile" : ""}`}
          style={{
            flex: 1,
            display: "flex",
            flexDirection: "column",
            minWidth: 0,
            overflow: "hidden",
            position: "relative",
          }}
        >
          {/* Mobile: Back to buckets button */}
          {selectedBucketName && (
            <div className="mobile-nav-bar">
              <button
                onClick={() => setMobileView("buckets")}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "5px",
                  background: "none",
                  border: "none",
                  cursor: "pointer",
                  fontSize: "13px",
                  fontWeight: 500,
                  color: "#1E40AF",
                  padding: "6px 12px",
                  letterSpacing: "-0.005em",
                }}
              >
                <ArrowLeft size={13} />
                Buckets
              </button>
            </div>
          )}

          <ObjectBrowser
            bucketName={selectedBucketName}
            prefix={currentPrefix}
            objects={objects}
            commonPrefixes={commonPrefixes}
            onNavigate={handleNavigate}
            onSelectObject={handleSelectObject}
            selectedObjectKey={selectedObject?.key ?? null}
            onRefresh={handleRefresh}
            onDeleteObject={(obj) => {
              const detail = getMockObjectDetail(selectedBucketName!, obj.key);
              setDeleteObject(detail);
            }}
          />
        </div>

        {/* ── Object Inspector ── */}
        {/* Desktop: always in 3rd column */}
        {/* Tablet: slide-over drawer */}
        {/* Mobile: full-screen view */}
        <div
          className={`inspector-wrapper ${mobileView !== "inspector" ? "inspector-hidden-mobile" : ""} ${isInspectorOpen ? "inspector-open-tablet" : ""}`}
        >
          {/* Mobile: back button */}
          <div className="mobile-nav-bar inspector-back-bar">
            <button
              onClick={() => {
                setMobileView("objects");
                setIsInspectorOpen(false);
              }}
              style={{
                display: "flex",
                alignItems: "center",
                gap: "5px",
                background: "none",
                border: "none",
                cursor: "pointer",
                fontSize: "13px",
                fontWeight: 500,
                color: "#1E40AF",
                padding: "6px 12px",
                letterSpacing: "-0.005em",
              }}
            >
              <ArrowLeft size={13} />
              Objects
            </button>
          </div>

          <ObjectInspector
            object={selectedObject}
            bucketName={selectedBucketName}
            onClose={() => {
              setSelectedObject(null);
              setIsInspectorOpen(false);
              setMobileView("objects");
            }}
            onDelete={setDeleteObject}
          />
        </div>

        {/* Tablet: toggle inspector button */}
        {selectedObject && (
          <button
            className="inspector-toggle-btn"
            onClick={() => setIsInspectorOpen(!isInspectorOpen)}
            title={isInspectorOpen ? "Close inspector" : "Open inspector"}
            style={{
              position: "absolute",
              right: isInspectorOpen ? "361px" : "8px",
              top: "10px",
              zIndex: 20,
              background: "#FFFFFF",
              border: "1px solid #E8E8E5",
              borderRadius: "10px",
              padding: "5px 7px",
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              gap: "4px",
              fontSize: "12px",
              color: "#737373",
              boxShadow: "0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.06)",
              transition: "all 0.15s ease",
            }}
          >
            <PanelRight size={13} />
          </button>
        )}
      </div>

      {/* Footer */}
      <Footer lastActivity={lastActivity} status={MOCK_STATUS} />

      {/* Dialogs */}
      {showCreateBucket && (
        <CreateBucketDialog
          onClose={() => setShowCreateBucket(false)}
          onCreate={handleCreateBucket}
        />
      )}
      {deleteBucket && (
        <DeleteBucketDialog
          bucket={deleteBucket}
          onClose={() => setDeleteBucket(null)}
          onConfirm={handleDeleteBucketConfirm}
        />
      )}
      {deleteObject && (
        <DeleteObjectDialog
          object={deleteObject}
          onClose={() => setDeleteObject(null)}
          onConfirm={handleDeleteObjectConfirm}
        />
      )}

      <style>{`
        /* Desktop: 3-pane always visible */
        @media (min-width: 1200px) {
          .bucket-sidebar-wrapper { display: flex !important; }
          .object-browser-wrapper { display: flex !important; }
          .inspector-wrapper {
            display: flex !important;
            flex-direction: column;
          }
          .mobile-nav-bar { display: none !important; }
          .inspector-toggle-btn { display: none !important; }
          .sidebar-hidden-mobile { display: flex !important; }
          .browser-hidden-mobile { display: flex !important; }
          .inspector-hidden-mobile { display: flex !important; }
        }

        /* Tablet: sidebar + browser, inspector as overlay */
        @media (min-width: 720px) and (max-width: 1199px) {
          .bucket-sidebar-wrapper { display: flex !important; }
          .sidebar-hidden-mobile { display: flex !important; }
          .browser-hidden-mobile { display: flex !important; }
          .object-browser-wrapper { display: flex !important; }
          .mobile-nav-bar { display: none !important; }
          .inspector-wrapper {
            position: absolute !important;
            right: 0;
            top: 0;
            bottom: 0;
            z-index: 30;
            transform: translateX(100%);
            transition: transform 0.2s ease;
            display: flex !important;
            flex-direction: column;
            box-shadow: -4px 0 20px rgba(0,0,0,0.12);
          }
          .inspector-open-tablet {
            transform: translateX(0) !important;
          }
          .inspector-hidden-mobile { display: flex !important; }
          .inspector-back-bar { display: none !important; }
        }

        /* Mobile: stacked views */
        @media (max-width: 719px) {
          .bucket-sidebar-wrapper {
            position: absolute !important;
            inset: 0;
            z-index: 10;
            background: #FAFAF9;
          }
          .sidebar-hidden-mobile {
            display: none !important;
          }
          .object-browser-wrapper {
            position: absolute !important;
            inset: 0;
            z-index: 10;
            background: #FAFAF9;
          }
          .browser-hidden-mobile {
            display: none !important;
          }
          .inspector-wrapper {
            position: absolute !important;
            inset: 0;
            z-index: 10;
            background: #FFFFFF;
            display: flex !important;
            flex-direction: column;
            width: 100% !important;
            min-width: unset !important;
          }
          .inspector-hidden-mobile {
            display: none !important;
          }
          .mobile-nav-bar {
            display: flex !important;
            background: #FFFFFF;
            border-bottom: 1px solid #E8E8E5;
            flex-shrink: 0;
          }
          .inspector-toggle-btn { display: none !important; }
        }

        /* Modern focus ring for interactive elements */
        button:focus-visible,
        input:focus-visible,
        select:focus-visible,
        textarea:focus-visible,
        a:focus-visible {
          outline: none;
          box-shadow: 0 0 0 3px rgba(16, 185, 129, 0.15);
        }
      `}</style>
    </div>
  );
}