//! Process-wide call controls, always bound to the originating account.

use egui::{Frame, Margin};

use crate::app::App;
use crate::model::{Action, CallPhase};
use crate::theme::{self, Icon};

use super::focus::{Stop, TabStop};

pub fn show(app: &mut App, ui: &mut egui::Ui) {
    let Some(call) = app.call.clone() else {
        return;
    };
    let palette = app.palette;
    // Linked windows have no separate macOS titlebar strip. Both rows must
    // clear its native buttons when this panel is above the chat headers.
    let inset = if app.is_linked() {
        theme::traffic_light_inset(ui.ctx())
    } else {
        0.0
    };
    let account = app
        .accounts
        .iter()
        .find(|account| account.id == call.account_id)
        .map(|account| account.display_label(app.locale))
        .unwrap_or_else(|| "WhatsApp".to_owned());
    egui::Panel::top("voice-call")
        .show_separator_line(false)
        .frame(Frame::new().fill(palette.panel).inner_margin(Margin {
            left: (14.0 + inset).min(f32::from(i8::MAX)) as i8,
            ..Margin::symmetric(14, 8)
        }))
        .show(ui, |ui| {
            if inset > 0.0 {
                super::titlebar_drag(ui, ui.max_rect());
            }
            ui.horizontal_wrapped(|ui| {
                theme::icon(ui, Icon::Phone, 18.0, palette.accent);
                theme::text(ui, &call.name, theme::semibold(14.0), palette.text);
                theme::text(ui, account, theme::regular(12.0), palette.secondary);
                theme::text(
                    ui,
                    phase_label(call.phase),
                    theme::medium(13.0),
                    palette.secondary,
                );
            });
            ui.horizontal_wrapped(|ui| {
                if call.phase == CallPhase::Incoming {
                    if theme::pill_button(ui, &palette, "Accept", true)
                        .tab_stop(Stop::CallAccept)
                        .clicked()
                    {
                        app.actions.push(Action::AcceptCall {
                            account_id: call.account_id.clone(),
                            call_id: call.id.clone(),
                        });
                    }
                    if theme::pill_button(ui, &palette, "Decline", false)
                        .tab_stop(Stop::CallEnd)
                        .clicked()
                    {
                        app.actions.push(Action::DeclineCall {
                            account_id: call.account_id.clone(),
                            call_id: call.id.clone(),
                        });
                    }
                } else {
                    if call.phase == CallPhase::Active
                        && theme::soft_button(
                            ui,
                            &palette,
                            Some(Icon::Mic),
                            if call.muted { "Unmute" } else { "Mute" },
                            call.muted,
                        )
                        .tab_stop(Stop::CallMute)
                        .clicked()
                    {
                        app.actions.push(Action::SetCallMuted {
                            account_id: call.account_id.clone(),
                            call_id: call.id.clone(),
                            muted: !call.muted,
                        });
                    }
                    ui.add_enabled_ui(call.phase != CallPhase::Ending, |ui| {
                        if theme::pill_button(ui, &palette, "Hang up", false)
                            .tab_stop(Stop::CallEnd)
                            .clicked()
                        {
                            app.actions.push(Action::HangUpCall {
                                account_id: call.account_id.clone(),
                                call_id: call.id.clone(),
                            });
                        }
                    });
                }
            });
        });
}

fn phase_label(phase: CallPhase) -> &'static str {
    match phase {
        CallPhase::Incoming => "Incoming voice call",
        CallPhase::Outgoing => "Calling…",
        CallPhase::Ringing => "Ringing…",
        CallPhase::Connecting => "Connecting…",
        CallPhase::Active => "Voice call connected",
        CallPhase::Ending => "Ending call…",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AccountId, ActiveCall};
    use crate::paths::AppDirs;
    use crate::settings::Settings;

    #[test]
    fn controls_target_the_call_account_even_when_another_account_is_visible() {
        for (phase, stop) in [
            (CallPhase::Incoming, Stop::CallAccept),
            (CallPhase::Incoming, Stop::CallEnd),
            (CallPhase::Active, Stop::CallMute),
            (CallPhase::Active, Stop::CallEnd),
        ] {
            let root = std::env::temp_dir().join(format!(
                "zapfast-call-ui-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let (mut app, _events) = App::headless(AppDirs::under(&root), Settings::default());
            let account_id = AccountId("other-account".into());
            let call_id = "fixture-call".to_owned();
            app.call = Some(ActiveCall {
                account_id: account_id.clone(),
                id: call_id.clone(),
                peer: "15550002222@s.whatsapp.net".into(),
                name: "Synthetic caller".into(),
                phase,
                muted: false,
            });
            assert_ne!(app.account().id, account_id);
            let ctx = egui::Context::default();
            app.attach(&ctx);
            let frame = |app: &mut App, events| {
                let mut output = ctx.run_ui(
                    egui::RawInput {
                        screen_rect: Some(egui::Rect::from_min_size(
                            egui::Pos2::ZERO,
                            egui::vec2(360.0, 600.0),
                        )),
                        events,
                        ..Default::default()
                    },
                    |ui| show(app, ui),
                );
                output.textures_delta.clear();
            };
            // Panels use their previous frame's height for hit testing. Let
            // the wrapped header and font atlas settle before choosing the
            // button's click position, as the full-window demo tests do.
            for _ in 0..3 {
                frame(&mut app, Vec::new());
            }
            let id = super::super::focus::stops(&ctx)
                .into_iter()
                .find(|(found, _)| *found == stop)
                .expect("call control is keyboard reachable")
                .1;
            let rect = ctx.read_response(id).unwrap().rect;
            assert!(ctx.content_rect().contains_rect(rect));
            let pos = rect.center();
            frame(&mut app, vec![egui::Event::PointerMoved(pos)]);
            let response = ctx.read_response(id).unwrap();
            assert_eq!(response.rect, rect, "the call controls have settled");
            assert!(response.hovered(), "the call control receives the pointer");
            for pressed in [true, false] {
                frame(
                    &mut app,
                    vec![
                        egui::Event::PointerMoved(pos),
                        egui::Event::PointerButton {
                            pos,
                            button: egui::PointerButton::Primary,
                            pressed,
                            modifiers: egui::Modifiers::NONE,
                        },
                    ],
                );
            }
            let expected = match (phase, stop) {
                (CallPhase::Incoming, Stop::CallAccept) => Action::AcceptCall {
                    account_id,
                    call_id,
                },
                (CallPhase::Incoming, Stop::CallEnd) => Action::DeclineCall {
                    account_id,
                    call_id,
                },
                (_, Stop::CallMute) => Action::SetCallMuted {
                    account_id,
                    call_id,
                    muted: true,
                },
                _ => Action::HangUpCall {
                    account_id,
                    call_id,
                },
            };
            assert_eq!(app.actions, [expected]);
        }
    }
}
