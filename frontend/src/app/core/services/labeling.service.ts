import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';
import { Label } from '../../models/label';

@Injectable({
  providedIn: 'root',
})
export class LabelingService {
  private labels$ = new BehaviorSubject<Label[]>([]);
  private selectedLabel$ = new BehaviorSubject<number>(-1);

  constructor() {}

  // todo - when to save labels (save == store in backend)
  // todo - add (video) id to fetch labels for specific video
  getLabels(): Observable<Label[]> {
    return this.labels$.asObservable();
  }

  getLabel(index): Label {
    return this.labels$.value[index];
  }

  addLabel(label: Label): void {
    const labels = this.labels$.value;
    this.labels$.next([...labels, label]);
  }

  deleteLabel(index: number): void {
    let labels = this.labels$.value;
    labels.splice(index, 1);
    this.labels$.next([...labels]);
  }

  updateLabel(index: number, label: Label): void {
    let labels = this.labels$.value;
    labels[index] = label;
    this.labels$.next([...labels]);
  }

  getSelectedLabelIndex(): Observable<number> {
    return this.selectedLabel$.asObservable();
  }

  setSelectedLabelIndex(index: number) {
    this.selectedLabel$.next(index);
  }
}
