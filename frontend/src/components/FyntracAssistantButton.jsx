import { useEffect, useState } from "react";
import FYNTRAC_MARK from "./fyntracMark";

/**
 * Fyntrac floating assistant button.
 * Dependency-free (plain React + one injected <style>), so it drops into the MUI app as-is.
 *
 * Props:
 *   open, onToggle   – control the Fyntrac Copilot panel from your parent
 *   thinking         – true while the assistant is streaming; ring spins faster
 *   unread           – number shown in the amber badge (0 hides it)
 *   shortcut         – enables Ctrl/⌘ + J to toggle (default true)
 *
 * While the panel is open the button fades out of the way (the panel carries
 * its own close control), and reappears the moment the panel is dismissed.
 */
const css = `
@property --fy-spin { syntax: '<angle>'; initial-value: 0deg; inherits: false; }
.fy-fab{position:fixed;right:24px;bottom:24px;z-index:1300;width:60px;height:60px;border-radius:50%;padding:3px;border:0;cursor:pointer;
  background:conic-gradient(from var(--fy-spin),#0FA3A3,#4F46E5,#FCA311,#0FA3A3);
  box-shadow:0 10px 30px rgba(20,33,61,.22),0 2px 6px rgba(20,33,61,.12);
  transition:transform .22s cubic-bezier(.22,1,.36,1),opacity .18s ease}
.fy-fab:hover{transform:translateY(-2px) scale(1.04);animation:fy-spin 2.4s linear infinite}
.fy-fab:active{transform:scale(.96)}
.fy-fab:focus-visible{outline:3px solid #4F46E5;outline-offset:4px}
.fy-fab.thinking{animation:fy-spin .9s linear infinite}
.fy-fab.open{opacity:0;pointer-events:none;transform:scale(.72) translateY(10px)}
.fy-core{width:100%;height:100%;border-radius:50%;background:#14213D;display:grid;place-items:center;position:relative;overflow:hidden}
.fy-core::after{content:"";position:absolute;inset:0;border-radius:50%;background:radial-gradient(120% 80% at 30% 15%,rgba(255,255,255,.18),transparent 55%)}
.fy-core svg{width:28px;height:28px;position:absolute;transition:transform .28s cubic-bezier(.3,1.4,.5,1),opacity .2s}
.fy-mark{width:38px!important;height:25px!important;margin-top:1px}
.fy-close{opacity:0;transform:rotate(-90deg) scale(.6)}
.fy-fab.open .fy-mark{opacity:0;transform:rotate(90deg) scale(.6)}
.fy-fab.open .fy-close{opacity:1;transform:none}
.fy-badge{position:absolute;top:-2px;right:-2px;min-width:20px;height:20px;padding:0 6px;border-radius:10px;background:#FCA311;color:#14213D;
  font:700 12px/20px 'Inter',system-ui,sans-serif;text-align:center;border:2px solid #fff}
@keyframes fy-spin{to{--fy-spin:360deg}}
@media (prefers-reduced-motion:reduce){.fy-fab,.fy-core svg{transition:none}.fy-fab:hover,.fy-fab.thinking{animation:none}}
`;

function useStyle() {
  useEffect(() => {
    if (document.getElementById("fy-fab-style")) return;
    const s = document.createElement("style");
    s.id = "fy-fab-style";
    s.textContent = css;
    document.head.appendChild(s);
  }, []);
}

const Mark = () => (
  <svg className="fy-mark" viewBox="0 0 160 104" aria-hidden="true">
    <image href={FYNTRAC_MARK} width="160" height="104" />
  </svg>
);

const Close = () => (
  <svg className="fy-close" viewBox="0 0 24 24" aria-hidden="true">
    <path d="M7 7l10 10M17 7L7 17" stroke="#fff" strokeWidth="2.4" strokeLinecap="round" fill="none" />
  </svg>
);

export default function FyntracAssistantButton({
  open: openProp,
  onToggle,
  thinking = false,
  unread = 0,
  shortcut = true,
}) {
  useStyle();
  const [openState, setOpenState] = useState(false);
  const open = openProp ?? openState;
  const toggle = () => (onToggle ? onToggle(!open) : setOpenState(!open));

  useEffect(() => {
    if (!shortcut) return;
    const h = (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "j") {
        e.preventDefault();
        toggle();
      }
    };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  });

  return (
    <button
      className={`fy-fab${open ? " open" : ""}${thinking ? " thinking" : ""}`}
      onClick={toggle}
      tabIndex={open ? -1 : 0}
      aria-hidden={open ? "true" : undefined}
      data-testid="fyntrac-assistant-button"
      aria-label={open ? "Close Fyntrac Copilot" : "Open Fyntrac Copilot"}
      aria-expanded={open}
      title="Ask Fyntrac Copilot (Ctrl/⌘ + J)"
    >
      <div className="fy-core">
        <Mark />
        <Close />
      </div>
      {!open && unread > 0 && <span className="fy-badge">{unread}</span>}
    </button>
  );
}
